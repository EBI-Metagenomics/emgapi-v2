import csv
import json
from collections import defaultdict
from datetime import timedelta
from enum import Enum
from pathlib import Path
from textwrap import dedent as _
from typing import List, Union

import django
import pandas as pd
from asgiref.sync import sync_to_async
from django.conf import settings
from django.db.models import QuerySet
from django.urls import reverse_lazy
from prefect import flow, get_run_logger, suspend_flow_run, task
from prefect.artifacts import create_table_artifact
from prefect.input import RunInput
from prefect.task_runners import SequentialTaskRunner

from workflows.prefect_utils.slurm_policies import (
    ResubmitWithCleanedNextflowIfFailedPolicy,
)

django.setup()

import analyses.models
import ena.models
from workflows.data_io_utils.filenames import (
    accession_prefix_separated_dir_path,
    file_path_shortener,
)
from workflows.ena_utils.ena_api_requests import (
    get_study_from_ena,
    get_study_readruns_from_ena,
)
from workflows.ena_utils.ena_file_fetching import convert_ena_ftp_to_fire_fastq
from workflows.flows.upload_assembly import upload_assembly
from workflows.nextflow_utils.samplesheets import (
    SamplesheetColumnSource,
    queryset_hash,
    queryset_to_samplesheet,
)
from workflows.prefect_utils.analyses_models_helpers import (
    chunk_list,
    task_mark_assembly_status,
)
from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash
from workflows.prefect_utils.slack_notification import notify_via_slack
from workflows.prefect_utils.slurm_flow import (
    ClusterJobFailedException,
    run_cluster_job,
)
from workflows.views import encode_samplesheet_path

EMG_CONFIG = settings.EMG_CONFIG


@task()
def get_memory_for_assembler(
    biome: analyses.models.Biome,
    assembler: analyses.models.Assembler,
):
    assembler_heuristics = analyses.models.ComputeResourceHeuristic.objects.filter(
        process=analyses.models.ComputeResourceHeuristic.ProcessTypes.ASSEMBLY,
        assembler=assembler,
    )

    # ascend the biome hierarchy to find a memory heuristic
    for biome_to_try in biome.ancestors().reverse():
        heuristic = assembler_heuristics.filter(biome=biome_to_try).first()
        if heuristic:
            return heuristic.memory_gb


class AssemblerChoices(str, Enum):
    # IDEA: it would be nice to sniff this from the pipeline schema
    pipeline_default = "pipeline_default"
    megahit = "megahit"
    metaspades = "metaspades"
    spades = "spades"


def get_biomes_as_choices():
    # IDEA: move this one to a helper of some sorts
    biomes = {
        str(biome.path): biome.pretty_lineage
        for biome in analyses.models.Biome.objects.all()
    }
    BiomeChoices = Enum("BiomeChoices", biomes)
    return BiomeChoices


@task(
    retries=2,
    task_run_name="Create/get assembly objects for read_runs in study: {study.accession}",
    log_prints=True,
)
def get_or_create_assemblies_for_runs(
    study: analyses.models.Study, read_runs: List[str]
) -> List[str]:
    assembly_ids = []
    for read_run in read_runs:
        run = analyses.models.Run.objects.get(ena_accessions__icontains=read_run)
        if run.experiment_type not in [run.ExperimentTypes.METAGENOMIC]:
            print(
                f"Not creating assembly for run {run.first_accession} because it is a {run.experiment_type}"
            )
            continue

        assembly, created = analyses.models.Assembly.objects.get_or_create(
            run=run, ena_study=study.ena_study, reads_study=study
        )
        if created:
            print(f"Created assembly {assembly}")
        assembly_ids.append(assembly.id)
    return assembly_ids


@task(
    log_prints=True,
)
def get_assemblies_to_attempt(study: analyses.models.Study) -> List[Union[str, int]]:
    """
    Determine the list of assemblies worth trying currently for this study.
    :param study:
    :return:
    """
    study.refresh_from_db()
    assemblies_worth_trying = study.assemblies_reads.exclude_by_statuses(
        [
            analyses.models.Assembly.AssemblyStates.PRE_ASSEMBLY_QC_FAILED,
            analyses.models.Assembly.AssemblyStates.ASSEMBLY_COMPLETED,
            analyses.models.Assembly.AssemblyStates.ASSEMBLY_BLOCKED,
        ]
    ).values_list("id", flat=True)
    print(f"Assemblies worth attempting are: {assemblies_worth_trying}")
    return assemblies_worth_trying


EXPERIMENT_TYPES_TO_MIASSEMBLER_LIBRARY_STRATEGY = defaultdict(
    lambda: "other",
    **{
        analyses.models.Run.ExperimentTypes.METAGENOMIC: "metagenomic",
        analyses.models.Run.ExperimentTypes.METATRANSCRIPTOMIC: "metatranscriptomic",
    },
)


@task(
    cache_key_fn=context_agnostic_task_input_hash,
)
def make_samplesheet(
    mgnify_study: analyses.models.Study,
    assembly_ids: List[Union[str, int]],
    assembler: analyses.models.Assembler,
) -> Path:
    assemblies = analyses.models.Assembly.objects.select_related("run").filter(
        id__in=assembly_ids
    )

    ss_hash = queryset_hash(assemblies, "id")

    memory = get_memory_for_assembler(mgnify_study.biome, assembler)

    sample_sheet_tsv = queryset_to_samplesheet(
        queryset=assemblies,
        filename=Path(EMG_CONFIG.slurm.default_workdir)
        / Path(
            f"{mgnify_study.ena_study.accession}_samplesheet_miassembler_{ss_hash}.csv"
        ),
        column_map={
            "study_accession": SamplesheetColumnSource(
                lookup_string="ena_study__accession"
            ),
            "reads_accession": SamplesheetColumnSource(
                lookup_string="run__ena_accessions",
                renderer=lambda accessions: accessions[0],
            ),
            "library_strategy": SamplesheetColumnSource(
                lookup_string="run__experiment_type",
                renderer=EXPERIMENT_TYPES_TO_MIASSEMBLER_LIBRARY_STRATEGY.get,
            ),
            "library_layout": SamplesheetColumnSource(
                lookup_string=f"run__metadata__{analyses.models.Run.CommonMetadataKeys.LIBRARY_LAYOUT}",
                renderer=lambda layout: str(layout).lower(),
            ),
            "fastq_1": SamplesheetColumnSource(
                lookup_string=f"run__metadata__{analyses.models.Run.CommonMetadataKeys.FASTQ_FTPS}",
                renderer=lambda ftps: convert_ena_ftp_to_fire_fastq(ftps[0]),
            ),
            "fastq_2": SamplesheetColumnSource(
                lookup_string=f"run__metadata__{analyses.models.Run.CommonMetadataKeys.FASTQ_FTPS}",
                renderer=lambda ftps: (
                    convert_ena_ftp_to_fire_fastq(ftps[1]) if len(ftps) > 1 else ""
                ),
            ),
            "assembler": SamplesheetColumnSource(
                lookup_string="id", renderer=lambda _: assembler.name.lower()
            ),
            "assembly_memory": SamplesheetColumnSource(
                lookup_string="id", renderer=lambda _: memory
            ),
        },
        bludgeon=True,
    )

    with open(sample_sheet_tsv) as f:
        csv_reader = csv.DictReader(f, delimiter=",")
        table = list(csv_reader)

    create_table_artifact(
        key="miassembler-initial-sample-sheet",
        table=table,
        description=_(
            f"""\
            Sample sheet created for run of MIAssembler.
            Saved to `{sample_sheet_tsv}`
            [Edit it]({EMG_CONFIG.service_urls.app_root}/workflows/edit-samplesheet/fetch/{encode_samplesheet_path(sample_sheet_tsv)})
            """
        ),
    )
    return sample_sheet_tsv


@task(
    cache_key_fn=context_agnostic_task_input_hash,
)
def make_samplesheets_for_runs_to_assemble(
    mgnify_study: analyses.models.Study,
    assembler: analyses.models.Assembler,
    chunk_size: int = 10,
) -> [Path]:
    assemblies_to_attempt = get_assemblies_to_attempt(mgnify_study)
    chunked_assemblies = chunk_list(assemblies_to_attempt, chunk_size)

    sheets = [
        make_samplesheet(mgnify_study, assembly_chunk, assembler)
        for assembly_chunk in chunked_assemblies
    ]
    return sheets


###################
# Assembler flow #
##################


@task
def update_assembly_metadata(
    miassembler_outdir: Path,
    assembly: analyses.models.Assembly,
    assembler: analyses.models.Assembler,
) -> None:
    """
    Update assembly with post-assembly metadata like assembler and coverage.
    """
    logger = get_run_logger()
    run_accession = assembly.run.first_accession
    study_accession = assembly.reads_study.ena_study.accession

    assembly.assembler = assembler
    assembly.dir = (
        miassembler_outdir
        / accession_prefix_separated_dir_path(study_accession, 7)
        / accession_prefix_separated_dir_path(run_accession, 7)
    )
    assembly.save()
    logger.info(f"Assembly directory is {assembly.dir}")

    coverage_report_path = Path(assembly.dir) / Path(
        f"assembly/{assembly.assembler.name.lower()}/{assembly.assembler.version}/coverage/{run_accession}_coverage.json"
    )
    if not coverage_report_path.is_file():
        raise Exception(f"Assembly coverage file not found at {coverage_report_path}")

    with open(coverage_report_path, "r") as json_file:
        coverage_report = json.load(json_file)

    for key in [
        assembly.CommonMetadataKeys.COVERAGE,
        assembly.CommonMetadataKeys.COVERAGE_DEPTH,
    ]:
        if not key in coverage_report:
            logger.warning(f"No '{key}' found in {coverage_report_path}")
        assembly.metadata[key] = coverage_report.get(key)

    logger.info(f"Assembly metadata of {assembly} is now {assembly.metadata}")

    assembly.save()


@flow(flow_run_name="Assemble {samplesheet_csv}", persist_result=True)
async def run_assembler_for_samplesheet(
    mgnify_study: analyses.models.Study,
    samplesheet_csv: Path,
    assembler: analyses.models.Assembler,
):
    samplesheet_df = pd.read_csv(samplesheet_csv, sep=",")
    assemblies: list[analyses.models.Assembly] = mgnify_study.assemblies_reads.filter(
        run__ena_accessions__0__in=samplesheet_df["reads_accession"]
    )

    async for assembly in assemblies:
        task_mark_assembly_status(
            assembly,
            status=assembly.AssemblyStates.ASSEMBLY_STARTED,
            unset_statuses=[assembly.AssemblyStates.ASSEMBLY_BLOCKED],
        )

    miassembler_outdir = Path(
        f"{EMG_CONFIG.slurm.default_workdir}/{mgnify_study.ena_study.accession}_miassembler"
    )
    command = (
        f"nextflow run {EMG_CONFIG.assembler.assembly_pipeline_repo} "
        f"-r {EMG_CONFIG.assembler.miassemebler_git_revision} "
        f"-latest "  # Pull changes from GitHub
        f"-profile {EMG_CONFIG.assembler.miassembler_nf_profile} "
        f"-resume "
        f"--samplesheet {samplesheet_csv} "
        f"--outdir {miassembler_outdir} "
        f"--assembler {assembler.name.lower()} "
        f"{'-with-tower' if settings.EMG_CONFIG.slurm.use_nextflow_tower else ''} "
        f"-name miassembler-samplesheet-{file_path_shortener(samplesheet_csv, 1, 15, True)} "
    )

    try:
        run_cluster_job(
            name=f"Assemble study {mgnify_study.ena_study.accession} via samplesheet {file_path_shortener(samplesheet_csv, 1, 15, True)}",
            command=command,
            expected_time=timedelta(
                days=EMG_CONFIG.assembler.assembly_pipeline_time_limit_days
            ),
            memory=f"{EMG_CONFIG.assembler.assembly_nextflow_master_job_memory_gb}G",
            environment="ALL,TOWER_ACCESS_TOKEN,TOWER_WORKSPACE_ID",
            input_files_to_hash=[samplesheet_csv],
            resubmit_policy=ResubmitWithCleanedNextflowIfFailedPolicy,
            working_dir=miassembler_outdir,
        )
    except ClusterJobFailedException:
        for assembly in assemblies:
            task_mark_assembly_status(
                assembly, status=analyses.models.Assembly.AssemblyStates.ASSEMBLY_FAILED
            )
    else:
        # The pipeline produces top level end of execution reports, which contain
        # the list of the runs that were assembled, and those that were not.
        # For more information: https://github.com/EBI-Metagenomics/miassembler?tab=readme-ov-file#top-level-reports

        # QC failed / not assembled runs: qc_failed_runs.csv
        # Assembled runs: assembled_runs.csv

        qc_failed_csv = miassembler_outdir / Path("qc_failed_runs.csv")
        qc_failed_runs = {}  # Stores {run_accession, qc_fail_reason}

        if qc_failed_csv.is_file():
            with qc_failed_csv.open(mode="r") as file_handle:
                for row in csv.reader(file_handle, delimiter=","):
                    run_accession, fail_reason = row
                    qc_failed_runs[run_accession] = fail_reason

        assembled_runs_csv = miassembler_outdir / Path("assembled_runs.csv")
        assembled_runs = set()

        if not assembled_runs_csv.is_file():
            for assembly in assemblies:
                task_mark_assembly_status(
                    assembly,
                    status=analyses.models.Assembly.AssemblyStates.ASSEMBLY_FAILED,
                    reason=f"The miassembler output is missing the {assembled_runs_csv} file.",
                )
            raise Exception(
                f"Missing end of execution assembled runs csv file. Expected path {assembled_runs_csv}."
            )

        with assembled_runs_csv.open(mode="r") as file_handle:
            for row in csv.reader(file_handle, delimiter=","):
                run_accession, assembler_software, assembler_version = row
                assembled_runs.add(run_accession)

        for assembly in assemblies:
            if assembly.run.first_accession in qc_failed_runs:
                task_mark_assembly_status(
                    assembly,
                    status=analyses.models.Assembly.AssemblyStates.PRE_ASSEMBLY_QC_FAILED,
                    reason=qc_failed_runs[assembly.run.first_accession],
                )
            elif assembly.run.first_accession in assembled_runs:
                task_mark_assembly_status(
                    assembly,
                    status=analyses.models.Assembly.AssemblyStates.ASSEMBLY_COMPLETED,
                    unset_statuses=[
                        analyses.models.Assembly.AssemblyStates.ASSEMBLY_FAILED
                    ],
                )
                update_assembly_metadata(miassembler_outdir, assembly, assembler)
            else:
                task_mark_assembly_status(
                    assembly,
                    status=analyses.models.Assembly.AssemblyStates.ASSEMBLY_FAILED,
                    reason="The assembly is missing from the pipeline end-of-run reports",
                )


@flow(
    log_prints=True,
    task_runner=SequentialTaskRunner,
    flow_run_name="Upload assemblies of: {study}",
)
async def upload_assemblies(study: analyses.models.Study, dry_run: bool = False):
    """
    Uploads all completed, not-previously-uploaded assemblies to ENA.
    The first assembly upload will usually trigger a TPA study to be created.
    """
    assemblies_to_upload: QuerySet = study.assemblies_reads.filter_by_statuses(
        [analyses.models.Assembly.AssemblyStates.ASSEMBLY_COMPLETED]
    ).exclude_by_statuses([analyses.models.Assembly.AssemblyStates.ASSEMBLY_UPLOADED])
    print(f"Will upload assemblies: {assemblies_to_upload.acount()}")
    async for assembly in assemblies_to_upload:
        await upload_assembly(assembly.id, dry_run=dry_run)


@flow(
    name="Assemble a study",
    flow_run_name="Assemble: {accession}",
    task_runner=SequentialTaskRunner,
)
async def assemble_study(
    accession: str, upload: bool = True, use_ena_dropbox_dev: bool = False
):
    """
    Get a study from ENA, and input it to MGnify.
    Kick off assembly pipeline.
    :param accession: Study accession e.g. PRJxxxxxx
    :param upload: Whether to upload the TPA study or not
    :param use_ena_dropbox_dev: Whether to use ENA wwwdev dropbox
    """
    logger = get_run_logger()

    # Create (or get) an ENA Study object, populating with metadata from ENA
    # Refresh from DB in case we get an old cached version.
    ena_study = await ena.models.Study.objects.get_ena_study(accession)
    if not ena_study:
        ena_study = await get_study_from_ena(accession)
        await ena_study.arefresh_from_db()
    logger.info(f"ENA Study is {ena_study.accession}: {ena_study.title}")

    # Get a MGnify Study object for this ENA Study
    mgnify_study = await analyses.models.Study.objects.get_or_create_for_ena_study(
        accession
    )

    logger.info(f"MGnify study is {mgnify_study.accession}: {mgnify_study.title}.")

    read_runs = get_study_readruns_from_ena(
        ena_study.accession,
        limit=5000,
        extra_cache_hash=ena_study.fetched_at.isoformat(),  # if ENA study is deleted/updated, the cache should be invalidated
    )
    logger.info(f"Have {len(read_runs)} from ENA portal API")

    # define this within flow because it dynamically creates options from DB.
    BiomeChoices = await sync_to_async(get_biomes_as_choices)()

    class BiomeAndAssemblerInput(RunInput):
        biome: BiomeChoices
        assembler: AssemblerChoices

    biome_and_assembler_input: BiomeAndAssemblerInput = await suspend_flow_run(
        wait_for_input=BiomeAndAssemblerInput.with_initial_data(
            assembler=AssemblerChoices.pipeline_default,
            description=f"""
**MI-Assembler**
This will assemble all {len(read_runs)} read-runs of study {ena_study.accession} \
using [MI-Assembler](https://www.github.com/ebi-metagenomics/mi-assembler).

Please pick which assembler tool to use (otherwise a default one will be picked depending on the data type).

**Biome tagger**
Please select a Biome for the entire study \
[{ena_study.accession}: {ena_study.title}](https://www.ebi.ac.uk/ena/browser/view/{ena_study.accession}).

The Biome is important metadata, and will also be used to guess how much memory is needed to assemble this study. \
These guesses are determined by the `ComputeResourceHeuristics`, \
which you can edit in the [admin panel]({EMG_CONFIG.service_urls.app_root}/{reverse_lazy("admin:index")}).
        """,
        )
    )

    biome = await analyses.models.Biome.objects.aget(
        path=biome_and_assembler_input.biome.name
    )

    mgnify_study.biome = biome
    await mgnify_study.asave()

    logger.info(f"MGnify study {mgnify_study.accession} has biome {biome.path}.")

    logger.info(f"Using assembler: {biome_and_assembler_input.assembler}")
    assembler_name = biome_and_assembler_input.assembler
    if assembler_name == AssemblerChoices.pipeline_default:
        assembler_name = analyses.models.Assembler.assembler_default

    assembler = (
        await analyses.models.Assembler.objects.filter(name__iexact=assembler_name)
        .order_by("-version")
        .afirst()
    )
    # assumes latest version...

    get_or_create_assemblies_for_runs(mgnify_study, read_runs)
    samplesheets = make_samplesheets_for_runs_to_assemble(mgnify_study, assembler)
    for samplesheet in samplesheets:
        await run_assembler_for_samplesheet(
            mgnify_study,
            samplesheet,
            assembler,
        )
    await notify_via_slack(f"Assembly of {mgnify_study} / {accession} is finished")

    if upload:
        await upload_assemblies(mgnify_study, dry_run=use_ena_dropbox_dev)
