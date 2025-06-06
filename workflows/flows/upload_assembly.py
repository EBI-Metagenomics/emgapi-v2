import gzip
import os
import re
from datetime import timedelta
from pathlib import Path
from typing import Optional

from assembly_uploader import assembly_manifest, study_xmls, submit_study
from Bio import SeqIO
from prefect.tasks import task_input_hash

from activate_django_first import EMG_CONFIG
from workflows.ena_utils.ena_accession_matching import ENA_ASSEMBLY_ACCESSION_REGEX

from workflows.prefect_utils.env_context import TemporaryEnv, UNSET
from workflows.prefect_utils.slurm_policies import ResubmitIfFailedPolicy

from django.core.exceptions import MultipleObjectsReturned, ObjectDoesNotExist
from prefect import flow, get_run_logger, task

import analyses.models
import ena.models
from workflows.prefect_utils.analyses_models_helpers import mark_assembly_status
from workflows.prefect_utils.slurm_flow import (
    ClusterJobFailedException,
    run_cluster_job,
)


OPTIONAL_SPADES_FILES = [
    ".assembly_graph.fastg.gz",
    ".assembly_graph_with_scaffolds.gfa.gz",
    ".scaffolds.fa.gz",
]
SPADES_PARAMS = "params.txt"


@task(
    retries=2,
    task_run_name="Sanity check: {assembly}",
)
def check_assembly(assembly: analyses.models.Assembly, assembly_path: Path):
    logger = get_run_logger()
    accession = assembly.run.first_accession
    logger.info(f"Check assembly {assembly}")
    # check filesize
    if not assembly_path.is_file():
        logger.warning(f"{assembly_path} does not exist")
        return False
    file_size = os.path.getsize(assembly_path)
    if not file_size:
        logger.warning(f"Contigs file is empty for assembly {assembly}")
        return False
    # check spades files
    if "spades" in assembly.assembler.name.lower():
        for postfix in OPTIONAL_SPADES_FILES:
            if not os.path.exists(
                os.path.join(os.path.dirname(assembly_path), accession + postfix)
            ):
                logger.warning(f"{accession + postfix} does not exist")
        if not os.path.exists(
            os.path.join(os.path.dirname(assembly_path), SPADES_PARAMS)
        ):
            logger.warning(f"{SPADES_PARAMS} does not exist")

    # check number of contigs
    count = 0
    with gzip.open(assembly_path, "rt") as handle:
        for _ in SeqIO.parse(handle, "fasta"):
            count += 1
            if count >= 2:
                return True
    if count < 2:
        logger.warning(
            f"Number of contigs in assembly file {assembly_path} is less than 2"
        )
        return False
    assembly.metadata[assembly.CommonMetadataKeys.N_CONTIGS] = count
    assembly.save()
    return True


def define_library(run_experiment_type):
    logger = get_run_logger()
    library = ""
    if run_experiment_type == analyses.models.Run.ExperimentTypes.METAGENOMIC:
        library = "metagenome"
    elif run_experiment_type == analyses.models.Run.ExperimentTypes.METATRANSCRIPTOMIC:
        library = "metatranscriptome"
    else:
        logger.warning(f"Unsupported experiment type {run_experiment_type}")
    return library


@task(
    retries=2,
    task_run_name="Create study XML: {study_accession}",
)
def create_study_xml(
    study_accession: str,
    library: str,
    output_dir: Path,
    is_third_party_assembly: bool = False,
    is_private: bool = False,
) -> (Path, str):
    logger = get_run_logger()

    assembly_study_writer = study_xmls.StudyXMLGenerator(
        study=study_accession,
        center_name=EMG_CONFIG.webin.submitting_center_name,
        library=library,
        tpa=is_third_party_assembly,
        private=is_private,
        output_dir=output_dir,
    )
    assembly_study_writer.write()
    assembly_study_title = assembly_study_writer._title

    # check was upload folder created or not
    written_to = assembly_study_writer.upload_dir
    logger.info(f"Upload folder: {os.path.abspath(written_to)}")
    if os.path.exists(written_to):
        if len([i for i in os.listdir(written_to) if i.endswith(".xml")]) != 2:
            raise Exception(
                f"Folder {output_dir} should contain 2 XMLs. Contained {os.listdir(written_to)}"
            )
        else:
            logger.info(f"Upload folder {written_to} and study XMLs were created")
    else:
        raise FileNotFoundError(f"Folder {written_to} does not exist")

    return written_to, assembly_study_title


@task(
    retries=2,
    task_run_name="Submit study XML: {study_accession}",
)
def submit_study_xml(
    study_accession: str, upload_dir: Path, dry_run: bool = True
) -> str:
    logger = get_run_logger()

    with TemporaryEnv(
        ENA_WEBIN=EMG_CONFIG.webin.emg_webin_account,
        ENA_WEBIN_PASSWORD=EMG_CONFIG.webin.emg_webin_password,
    ):
        registered_study_accession = submit_study.submit_study(
            study_accession, is_test=dry_run, directory=upload_dir
        )

    logger.info(f"Assembly study was registered as {registered_study_accession}")

    return registered_study_accession


@task(
    retries=2,
    cache_key_fn=task_input_hash,
    task_run_name="Check study registration, create study XML and submit to ENA",
)
def handle_tpa_study(
    assembly: analyses.models.Assembly,
    upload_folder: Path,
    dry_run: bool,
):
    logger = get_run_logger()

    # ensure we have appropriate accessions
    assembly.reads_study.inherit_accessions_from_related_ena_object("ena_study")

    # if the assembly study (i.e. TPA study for public data) does not yet have an ENA study attached,
    # it means we need to register it.

    # check if any assemblies in the same reads study as this assembly have an assembly study already
    assembly_study = None
    assembly_from_same_reads_study_with_assembly_study = (
        assembly.reads_study.assemblies_reads.filter(
            assembly_study__isnull=False
        ).first()
    )
    if assembly_from_same_reads_study_with_assembly_study:
        logger.info(
            f"Found {assembly_study = } on {assembly_from_same_reads_study_with_assembly_study}, "
            f"which came from same reads study {assembly.reads_study} as {assembly}. "
            f"Will assign that as assembly study."
        )
        assembly_study = (
            assembly_from_same_reads_study_with_assembly_study.assembly_study
        )

    if not (assembly_study and assembly_study.ena_study):
        logger.info(
            f"Need to register an ENA (assembly) study for the assemblies of {assembly.reads_study.first_accession}"
        )
        study_reg_dir, assembly_study_title = create_study_xml(
            study_accession=assembly.reads_study.first_accession,
            library=define_library(assembly.run.experiment_type),
            output_dir=upload_folder,
            is_third_party_assembly=True,
            is_private=False,
        )

        registered_study = submit_study_xml(
            study_accession=assembly.reads_study.first_accession,
            upload_dir=study_reg_dir,
            dry_run=dry_run,
        )
        logger.info(f"Study submitted successfully under {registered_study}")

        newly_registered_ena_study, created = ena.models.Study.objects.get_or_create(
            accession=registered_study, title=assembly_study_title
        )
        if created:
            logger.info(f"Created new ENA study {newly_registered_ena_study}")

        new_mgnify_study, created = analyses.models.Study.objects.get_or_create(
            ena_study=newly_registered_ena_study,
            defaults={
                "title": newly_registered_ena_study.title,
                "biome": assembly.reads_study.biome,
                "ena_accessions": [registered_study],
            },
        )
        if created:
            logger.info(f"Created new MGnify study {new_mgnify_study}")

        assembly_study = new_mgnify_study
    assembly.assembly_study = assembly_study
    assembly.save()

    return assembly_study


@task(
    retries=2,
    task_run_name="Generate csv for upload: {assembly}",
)
def generate_assembly_csv(
    metadata_dir: Path, assembly: analyses.models.Assembly, assembly_path: Path
):
    logger = get_run_logger()

    assembly_csv = metadata_dir / Path(f"{assembly.run.first_accession}.csv")
    with open(assembly_csv, "w") as file_out:
        file_out.write(
            ",".join(["Run", "Coverage", "Assembler", "Version", "Filepath"]) + "\n"
        )
        line = ",".join(
            [
                assembly.run.first_accession,
                str(assembly.metadata.get(assembly.CommonMetadataKeys.COVERAGE_DEPTH)),
                assembly.assembler.name,
                str(assembly.assembler.version),
                os.path.abspath(assembly_path),
            ]
        )
        file_out.write(line + "\n")
    logger.info(f"CSV: {os.path.abspath(assembly_csv)}")
    return assembly_csv


@task(
    retries=2,
    task_run_name="Create metadata and assembly manifest",
)
def prepare_assembly(
    mgnify_assembly: analyses.models.Assembly,
    assembly_path: Path,
    upload_folder: Path,
) -> Path:
    logger = get_run_logger()

    # create metadata table
    metadata_dir = upload_folder / Path("metadata")
    metadata_dir.mkdir(exist_ok=True)

    logger.info(f"Will generate csv for assembly {mgnify_assembly}")
    data_csv_path = generate_assembly_csv(
        metadata_dir=metadata_dir,
        assembly=mgnify_assembly,
        assembly_path=assembly_path,
    )
    # assembly_uploader assembly_manifest
    logger.info("Will generate assembly manifests")

    with TemporaryEnv(
        ENA_WEBIN=(
            f"{EMG_CONFIG.webin.broker_prefix}{mgnify_assembly.reads_study.webin_submitter}"
            if mgnify_assembly.is_private
            else UNSET
        ),
        ENA_WEBIN_PASSWORD=(
            EMG_CONFIG.webin.broker_password if mgnify_assembly.is_private else UNSET
        ),
    ):
        assembly_manifest_writer = assembly_manifest.AssemblyManifestGenerator(
            study=mgnify_assembly.reads_study.first_accession,
            assembly_study=mgnify_assembly.assembly_study.first_accession,
            assemblies_csv=data_csv_path,
            output_dir=upload_folder,
            private=mgnify_assembly.is_private,
            tpa=mgnify_assembly.assembly_study.id != mgnify_assembly.reads_study.id,
        )
        assembly_manifest_writer.write()

    manifest = assembly_manifest_writer.upload_dir / Path(
        mgnify_assembly.run.first_accession + ".manifest"
    )
    return manifest


@task(
    retries=2,
    task_run_name="Get assembly accession for {assembly}",
)
def get_assigned_assembly_accession(assembly: analyses.models.Assembly, manifest: Path):
    logger = get_run_logger()
    logger.info(f"Getting assembly accession for {assembly}")
    webin_cli_log = manifest.parent / Path("webin-cli.report")
    logger.info(f"Looking for {webin_cli_log}")
    if os.path.exists(webin_cli_log):
        with open(webin_cli_log, "r") as report:
            report_lines = report.readlines()
        assembly_accession_match_list = re.findall(
            ENA_ASSEMBLY_ACCESSION_REGEX, ",".join(report_lines)
        )
        if assembly_accession_match_list:
            logger.info(f"Got {assembly_accession_match_list[0]}")
            return assembly_accession_match_list[0]
        else:
            logger.warning(f"No accession found in {webin_cli_log}")
            return None
    else:
        logger.warning(f"No {webin_cli_log} found")
        return None


@task()
def add_erz_accession(assembly: analyses.models.Assembly, erz_accession):
    assembly.add_erz_accession(erz_accession)


@task(
    retries=2,
    cache_key_fn=task_input_hash,
    task_run_name="Run Webin-cli to upload {mgnify_assembly}",
)
def submit_assembly_slurm(
    manifest: Path,
    mgnify_assembly: analyses.models.Assembly,
    dry_run: bool,
):
    username = (
        f"{EMG_CONFIG.webin.broker_prefix}{mgnify_assembly.reads_study.webin_submitter}"
        if mgnify_assembly.is_private
        else EMG_CONFIG.webin.emg_webin_account
    )
    password = (
        EMG_CONFIG.webin.broker_password
        if mgnify_assembly.is_private
        else EMG_CONFIG.webin.emg_webin_password
    )
    logger = get_run_logger()
    xms = int(EMG_CONFIG.assembler.assembly_uploader_mem_gb / 2)
    xmx = int(EMG_CONFIG.assembler.assembly_uploader_mem_gb)
    command = (
        f"java -Xms{xms}g -Xmx{xmx}g -jar {EMG_CONFIG.webin.webin_cli_executor} "
        f"-context=genome "
        f"-manifest={manifest} "
        f"-userName='{username}' "
        f"-password='{password}' "
    )
    if dry_run:
        command += "-test -validate "
    else:
        command += "-submit "

    try:
        run_cluster_job.with_options(
            retries=EMG_CONFIG.webin.webin_cli_retries,
            retry_delay_seconds=EMG_CONFIG.webin.webin_cli_retry_delay_seconds,
        )(
            name=f"Upload assembly for {mgnify_assembly} to ENA",
            command=command,
            expected_time=timedelta(
                hours=EMG_CONFIG.assembler.assembly_uploader_time_limit_hrs
            ),
            memory=f"{EMG_CONFIG.assembler.assembly_uploader_mem_gb}G",
            environment="ALL",  # copy env vars from the prefect agent into the slurm job
            input_files_to_hash=[manifest],
            resubmit_policy=ResubmitIfFailedPolicy,
            working_dir=manifest.parent,
        )
    except ClusterJobFailedException:
        logger.error(
            f"Something went wrong running webin-cli upload for {mgnify_assembly}"
        )
        mark_assembly_status(
            mgnify_assembly,
            status=mgnify_assembly.AssemblyStates.ASSEMBLY_UPLOAD_FAILED,
        )
    else:
        logger.info(f"Successfully ran webin-cli upload for {mgnify_assembly}")
        if dry_run:
            # no webin.report generated
            mark_assembly_status(
                mgnify_assembly,
                status=mgnify_assembly.AssemblyStates.ASSEMBLY_UPLOADED,
                unset_statuses=[mgnify_assembly.AssemblyStates.ASSEMBLY_UPLOAD_FAILED],
            )
        else:
            # check webin.report for ERZ
            erz_accession = get_assigned_assembly_accession(mgnify_assembly, manifest)
            if erz_accession:
                logger.info(f"Upload completed for {mgnify_assembly}")
                add_erz_accession(mgnify_assembly, erz_accession)
                mark_assembly_status(
                    mgnify_assembly,
                    status=mgnify_assembly.AssemblyStates.ASSEMBLY_UPLOADED,
                    unset_statuses=[
                        mgnify_assembly.AssemblyStates.ASSEMBLY_UPLOAD_FAILED
                    ],
                )
            else:
                logger.info(f"Upload failed for {mgnify_assembly}")
                mark_assembly_status(
                    mgnify_assembly,
                    status=mgnify_assembly.AssemblyStates.ASSEMBLY_UPLOAD_FAILED,
                )


##########################
# Assembly uploader flow #
##########################


@flow(
    name="Sanity check and upload an assembly",
    flow_run_name="Sanity check and upload",
)
def upload_assembly(
    assembly_id: int, dry_run: bool = True, custom_upload_folder: Optional[Path] = None
):
    """
    This flow performs a sanity check and uploads an assembly for a specific run to ENA.

    It is intended to be executed *per run* after the assembly flow. The assembly uploader
    is a separate python library to prepare the upload files. The assembly submission
    via `webin-cli` is launched as a SLURM cluster job.
    """
    logger = get_run_logger()

    if custom_upload_folder:
        logger.warning(f"Using non-standard upload directory: {custom_upload_folder}")

    try:
        mgnify_assembly: analyses.models.Assembly = (
            analyses.models.Assembly.objects.get(
                id=assembly_id,
            )
        )
    except (MultipleObjectsReturned, ObjectDoesNotExist) as e:
        logger.error(f"Problem getting assembly {assembly_id = }")
        raise e

    assembly_path = Path(mgnify_assembly.dir_with_miassembler_suffix) / Path(
        f"{mgnify_assembly.run.first_accession}_cleaned.contigs.fa.gz"
    )

    upload_folder = custom_upload_folder or assembly_path.parent / Path("upload")
    upload_folder.mkdir(exist_ok=True, parents=True)

    logger.info(f"Assembly ID:{mgnify_assembly.id} found in {mgnify_assembly.dir}")
    logger.info(f"Using {upload_folder = }")

    # Sanity check
    logger.info(f"Processing assembly for: {mgnify_assembly.run.first_accession}")
    if check_assembly(mgnify_assembly, assembly_path):
        logger.info(f"Assembly {mgnify_assembly} passed sanity check")
    else:
        mark_assembly_status(
            mgnify_assembly,
            status=mgnify_assembly.AssemblyStates.POST_ASSEMBLY_QC_FAILED,
        )
        logger.error(
            f"Assembly {mgnify_assembly} did not pass sanity check. No further action."
        )
        return

    if (
        mgnify_assembly.is_private or mgnify_assembly.reads_study.is_private
    ):  # should be the same really
        # we will broker assembly into the original reads study
        mgnify_assembly.assembly_study = mgnify_assembly.reads_study
        mgnify_assembly.save()
    else:
        # Register TPA study and submit to ENA, unless it was made before by another assembly in this reads study
        handle_tpa_study(
            mgnify_assembly,
            upload_folder,
            dry_run,
        )

    # Create assembly manifest
    manifest = prepare_assembly(
        mgnify_assembly,
        assembly_path,
        upload_folder,
    )

    # Submit assembly with Webin-cli
    submit_assembly_slurm(manifest, mgnify_assembly, dry_run)
