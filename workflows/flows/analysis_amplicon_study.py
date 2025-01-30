import csv
import re
from datetime import timedelta
from pathlib import Path
from textwrap import dedent as _
from typing import List, Union

import django
from django.db.models import QuerySet

from workflows.prefect_utils.slurm_policies import ResubmitIfFailedPolicy

django.setup()

from django.conf import settings
from django.utils.text import slugify
from prefect import flow, get_run_logger, task
from prefect.artifacts import create_table_artifact
from prefect.task_runners import SequentialTaskRunner

import analyses.models
import ena.models
from workflows.data_io_utils.mgnify_v6_utils.amplicon import import_qc, import_taxonomy
from workflows.ena_utils.ena_api_requests import (
    get_study_from_ena,
    get_study_readruns_from_ena,
)
from workflows.ena_utils.ena_file_fetching import convert_ena_ftp_to_fire_fastq
from workflows.nextflow_utils.samplesheets import (
    SamplesheetColumnSource,
    queryset_hash,
    queryset_to_samplesheet,
)
from workflows.prefect_utils.analyses_models_helpers import (
    chunk_list,
    task_mark_analysis_status,
)
from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash
from workflows.prefect_utils.slurm_flow import (
    ClusterJobFailedException,
    run_cluster_job,
)
from workflows.views import encode_samplesheet_path

FASTQ_FTPS = analyses.models.Run.CommonMetadataKeys.FASTQ_FTPS
METADATA__FASTQ_FTPS = f"{analyses.models.Run.metadata.field.name}__{FASTQ_FTPS}"
EMG_CONFIG = settings.EMG_CONFIG
AnalysisStates = analyses.models.Analysis.AnalysisStates


@task(
    log_prints=True,
)
def get_analyses_to_attempt(
    study: analyses.models.Study,
    for_experiment_type: analyses.models.WithExperimentTypeModel.ExperimentTypes,
) -> List[Union[str, int]]:
    """
    Determine the list of runs worth trying currently for this study.
    :param study: MGYS study to look for to-be-completed analyses in
    :param for_experiment_type: E.g. AMPLICON or WGS
    :return: List of analysis object IDs
    """
    study.refresh_from_db()
    analyses_worth_trying = (
        study.analyses.exclude_by_statuses(
            [
                analyses.models.Analysis.AnalysisStates.ANALYSIS_QC_FAILED,
                analyses.models.Analysis.AnalysisStates.ANALYSIS_COMPLETED,
                analyses.models.Analysis.AnalysisStates.ANALYSIS_BLOCKED,
            ]
        )
        .filter(experiment_type=for_experiment_type.value)
        .order_by("id")
        .values_list("id", flat=True)
    )

    print(f"Got {len(analyses_worth_trying)} analyses to attempt")
    return analyses_worth_trying


@task(
    log_prints=True,
)
def create_analyses(
    study: analyses.models.Study,
    for_experiment_type: analyses.models.WithExperimentTypeModel.ExperimentTypes,
    pipeline: analyses.models.Analysis.PipelineVersions = analyses.models.Analysis.PipelineVersions.v6,
) -> List[analyses.models.Analysis]:
    """
    Get or create analysis objects for each run in the study that matches the given experiment type.
    :param study: An MGYS study that already has runs to be analysed attached.
    :param for_experiment_type: E.g. AMPLICON or WGS
    :param pipeline: Pipeline version e.g. v6
    :return: List of matching/created analysis objects.
    """
    analyses_list = []
    for run in study.runs.filter(experiment_type=for_experiment_type):
        analysis, created = analyses.models.Analysis.objects.get_or_create(
            study=study,
            sample=run.sample,
            run=run,
            ena_study=study.ena_study,
            pipeline_version=pipeline,
        )
        if created:
            print(
                f"Created analyses {analysis} {analysis.run.first_accession} {analysis.run.experiment_type}"
            )
        analysis.inherit_experiment_type()
        analyses_list.append(analysis)
    return analyses_list


@task(log_prints=True)
def mark_analysis_as_started(analysis: analyses.models.Analysis):
    analysis.mark_status(AnalysisStates.ANALYSIS_STARTED)


@task(log_prints=True)
def mark_analysis_as_failed(analysis: analyses.models.Analysis):
    analysis.mark_status(AnalysisStates.ANALYSIS_FAILED)


@task(
    cache_key_fn=context_agnostic_task_input_hash,
    log_prints=True,
)
def make_samplesheet_amplicon(
    mgnify_study: analyses.models.Study,
    amplicon_analyses: QuerySet,
) -> (Path, str):
    """
    Makes a samplesheet CSV file for a set of amplicon analyses, suitable for amplicon pipeline.
    :param mgnify_study: MGYS study
    :param amplicon_analyses: QuerySet of the amplicon analyses to be executed
    :return: Tuple of the Path to the samplesheet file, and a hash of the run IDs which is used in the SS filename.
    """

    runs_ids = amplicon_analyses.values_list("run_id", flat=True)
    runs = analyses.models.Run.objects.filter(id__in=runs_ids)
    print(f"Making amplicon samplesheet for runs {runs_ids}")

    ss_hash = queryset_hash(runs, "id")

    sample_sheet_csv = queryset_to_samplesheet(
        queryset=runs,
        filename=Path(EMG_CONFIG.slurm.default_workdir)
        / Path(
            f"{mgnify_study.ena_study.accession}_samplesheet_amplicon-v6_{ss_hash}.csv"
        ),
        column_map={
            "sample": SamplesheetColumnSource(
                lookup_string="ena_accessions",
                renderer=lambda accessions: accessions[0],
            ),
            "fastq_1": SamplesheetColumnSource(
                lookup_string=METADATA__FASTQ_FTPS,
                renderer=lambda ftps: convert_ena_ftp_to_fire_fastq(ftps[0]),
            ),
            "fastq_2": SamplesheetColumnSource(
                lookup_string=METADATA__FASTQ_FTPS,
                renderer=lambda ftps: (
                    convert_ena_ftp_to_fire_fastq(ftps[1]) if len(ftps) > 1 else ""
                ),
            ),
            "single_end": SamplesheetColumnSource(
                lookup_string=METADATA__FASTQ_FTPS,
                renderer=lambda ftps: "false" if len(ftps) > 1 else "true",
            ),
        },
        bludgeon=True,
    )

    with open(sample_sheet_csv) as f:
        csv_reader = csv.DictReader(f, delimiter=",")
        table = list(csv_reader)

    create_table_artifact(
        key="amplicon-v6-initial-sample-sheet",
        table=table,
        description=_(
            f"""\
            Sample sheet created for run of amplicon-v6.
            Saved to `{sample_sheet_csv}`
            [Edit it]({EMG_CONFIG.service_urls.app_root}/workflows/edit-samplesheet/fetch/{encode_samplesheet_path(sample_sheet_csv)})
            """
        ),
    )
    return sample_sheet_csv, ss_hash


@task(
    cache_key_fn=context_agnostic_task_input_hash,
)
def sanity_check_amplicon_results(
    amplicon_current_outdir: Path, analysis: analyses.models.Analysis
):
    """
    QC folder:
        required:
         - ${run_id}_seqfu.tsv
        optional:
         - ${run_id}.merged.fastq.gz / ${run_id}.fastp.fastq.gz
         - ${run_id}.fastp.json
         - ${run_id}_suffix_header_err.json
         - ${run_id}_multiqc_report.html
    SEQUENCE CATEGORISATION folder:
        required:
         - ${run_id}_${gene}.fasta (depending on if the gene was SSU/LSU/ITS)
         - ${run_id}.tblout.deoverlapped
         - ${run_id}_${gene}_rRNA_${domain}.${domain_id}.fa (domain can be bacteria/archaea/eukarya)
    AMPLIFIED REGION INFERENCE folder:
        required:
         - ${run_id}.tsv
        optional:
         - ${run_id}.*S.${V?}.txt - max 2 files, if passed inference thresholds, example, ERR4334351.16S.V3-V4.txt
    PRIMER IDENTIFICATION folder:
        if only required file present - it should be empty
        if 3 files are present they can be all not empty or all empty
        required:
         - ${run_id}.cutadapt.json - if ony that file it should be empty
        optional (if ${run_id}.cutadapt.json not empty):
         - ${run_id}_primers.fasta
         - ${run_id}_primer_validation.tsv
    ASV:
        required:
         - ${run_id}_dada2_stats.tsv
         - ${run_id}_DADA2-SILVA_asv_tax.tsv
         - ${run_id}_DADA2-PR2_asv_tax.tsv
         - ${run_id}_asv_seqs.fasta
         - /${var_region}
         - /${var_region}/${run_id}_${var_region}_asv_read_counts.tsv
        optional:
         - second var region
         - concat (for both var regions)
    TAXONOMY SUMMARY:
        optional:
         - SILVA-SSU
         - PR2
         - UNITE
         - ITSoneDB
            - {run_id}.html
            - ${run_id}_{db_label}.mseq
            - ${run_id}_{db_label}.tsv
            - ${run_id}_${db_label}.txt
         - DADA2-SILVA
         - DADA2-PR2
            - ${run_id}_${db_label}.mseq
            for 1 var region:
             - ${run_id}_${var_region}_{db_label}_asv_krona_counts.txt
             - ${run_id}_${var_region}.html
            for 2 var regions:
             - ${run_id}_${var_region1}_{db_label}_asv_krona_counts.txt
             - ${run_id}_${var_region1}.html
             - ${run_id}_${var_region2}_{db_label}_asv_krona_counts.txt
             - ${run_id}_${var_region2}.html
             - ${run_id}_concat_{db_label}_asv_krona_counts.txt
             - ${run_id}_concat.html
    """
    logger = get_run_logger()
    reason = None
    run_id = analysis.run.first_accession
    qc_folder = Path(
        f"{amplicon_current_outdir}/{EMG_CONFIG.amplicon_pipeline.qc_folder}"
    )
    sequence_categorisation_folder = Path(
        f"{amplicon_current_outdir}/{EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder}"
    )
    amplified_region_inference_folder = Path(
        f"{amplicon_current_outdir}/{EMG_CONFIG.amplicon_pipeline.amplified_region_inference_folder}"
    )
    primer_identification_folder = Path(
        f"{amplicon_current_outdir}/{EMG_CONFIG.amplicon_pipeline.primer_identification_folder}"
    )
    asv_folder = Path(
        f"{amplicon_current_outdir}/{EMG_CONFIG.amplicon_pipeline.asv_folder}"
    )
    taxonomy_summary_folder = Path(
        f"{amplicon_current_outdir}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}"
    )

    # SEQUENCE CATEGORISATION optional folder
    if sequence_categorisation_folder.exists():
        # if folder exists - checking for required files
        pattern_gene_fasta = re.compile(r"\w+_(SSU|LSU|ITS)\.fasta$")
        matching_gene_files = [
            True if f.is_file() and pattern_gene_fasta.match(f.name) else False
            for f in sequence_categorisation_folder.iterdir()
        ]
        pattern_domain_fasta = re.compile(
            r"\w+_(SSU|LSU|ITS)_rRNA_(bacteria|archaea|eukarya)\.[A-Z0-9]+\.fa$"
        )
        matching_domain_files = [
            True if f.is_file() and pattern_domain_fasta.match(f.name) else False
            for f in sequence_categorisation_folder.iterdir()
        ]
        if not (
            Path(
                f"{sequence_categorisation_folder}/{run_id}.tblout.deoverlapped"
            ).exists()
            and sum(matching_gene_files)
            and sum(matching_domain_files)
        ):
            reason = f"missing required files in {EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder}"
            logger.info(f"Post sanity check for {run_id}: {reason}")

    amplified_regions = []
    # AMPLIFIED REGION INFERENCE optional folder
    if amplified_region_inference_folder.exists():
        if not Path(f"{amplified_region_inference_folder}/{run_id}.tsv").exists():
            reason = f"missing required file in {EMG_CONFIG.amplicon_pipeline.amplified_region_inference_folder}"
            logger.info(f"Post sanity check for {run_id}: {reason}")
        else:
            if len(list(amplified_region_inference_folder.iterdir())) > 1:
                # extract variable regions
                pattern_txt = re.compile(r"\w+\.(\w+S)\.(V[\w.-]+)\.txt$")
                for f in amplified_region_inference_folder.iterdir():
                    match = pattern_txt.search(f.name)
                    if match:
                        amplified_regions.append(f"{match.group(1)}-{match.group(2)}")
                if len(amplified_regions) > 2:
                    reason = "More than 2 variable regions were found"
                    logger.info(f"Post sanity check for {run_id}: {reason}")

    # PRIMER IDENTIFICATION optional folder
    if primer_identification_folder.exists():
        cutadapt_json = Path(f"{primer_identification_folder}/{run_id}.cutadapt.json")
        if len(list(primer_identification_folder.iterdir())) == 1:
            if not cutadapt_json.exists():
                # checking required file
                reason = f"missing required file in {EMG_CONFIG.amplicon_pipeline.primer_identification_folder}"
                logger.info(f"Post sanity check for {run_id}: {reason}")
            else:
                # checking it should be empty
                if cutadapt_json.stat().st_size:
                    reason = f"required file in {EMG_CONFIG.amplicon_pipeline.primer_identification_folder} did not passed sanity check"
                    logger.info(f"Post sanity check for {run_id}: {reason}")
        elif len(list(primer_identification_folder.iterdir())) == 3:
            primers_file = Path(
                f"{primer_identification_folder}/{run_id}_primers.fasta"
            )
            validation_file = Path(
                f"{primer_identification_folder}/{run_id}_primer_validation.tsv"
            )
            if (
                primers_file.exists()
                and validation_file.exists()
                and cutadapt_json.exists()
            ):
                if not (
                    (
                        primers_file.stat().st_size == 0
                        and validation_file.stat().st_size == 0
                        and cutadapt_json.stat().st_size == 0
                    )
                    or (
                        primers_file.stat().st_size != 0
                        and validation_file.stat().st_size != 0
                        and cutadapt_json.stat().st_size != 0
                    )
                ):
                    reason = f"Incorrect file sizes in {EMG_CONFIG.amplicon_pipeline.primer_identification_folder}"
                    logger.info(f"Post sanity check for {run_id}: {reason}")
            else:
                reason = f"Incorrect structure of {EMG_CONFIG.amplicon_pipeline.primer_identification_folder}"
                logger.info(f"Post sanity check for {run_id}: {reason}")
        else:
            reason = f"Incorrect number of files in {EMG_CONFIG.amplicon_pipeline.primer_identification_folder}"
            logger.info(f"Post sanity check for {run_id}: {reason}")

    # ASV optional folder
    if asv_folder.exists():
        dada2_stats = Path(f"{asv_folder}/{run_id}_dada2_stats.tsv")
        dada2_silva = Path(f"{asv_folder}/{run_id}_DADA2-SILVA_asv_tax.tsv")
        dada2_pr2 = Path(f"{asv_folder}/{run_id}_DADA2-PR2_asv_tax.tsv")
        asv_stats = Path(f"{asv_folder}/{run_id}_asv_seqs.fasta")
        if not (
            dada2_stats.exists()
            and dada2_pr2.exists()
            and dada2_silva.exists()
            and asv_stats.exists()
        ):
            reason = (
                f"missing required file in {EMG_CONFIG.amplicon_pipeline.asv_folder}"
            )
            logger.info(f"Post sanity check for {run_id}: {reason}")
        else:
            # check var regions
            if amplified_regions:
                for region in amplified_regions:
                    if Path(f"{asv_folder}/{region}").exists():
                        if not Path(
                            f"{asv_folder}/{region}/{run_id}_{region}_asv_read_counts.tsv"
                        ).exists():
                            reason = f"No asv_read_counts in {region}"
                    else:
                        reason = (
                            f"No {region} in {EMG_CONFIG.amplicon_pipeline.asv_folder}"
                        )
            # check concat folder for more than 1 region
            if len(amplified_regions) > 1:
                if Path(f"{asv_folder}/concat").exists():
                    if not Path(
                        f"{asv_folder}/concat/{run_id}_concat_asv_read_counts.tsv"
                    ).exists():
                        reason = f"No counts for concat folder in {EMG_CONFIG.amplicon_pipeline.asv_folder}"
                else:
                    reason = f"Missing concat folder in {EMG_CONFIG.amplicon_pipeline.asv_folder} for {len(amplified_regions)} regions"

    # TAXONOMY SUMMARY folder:
    if taxonomy_summary_folder.exists():
        dada2_tax_names = ["DADA2-SILVA", "DADA2-PR2"]
        tax_dbs = ["SILVA-SSU", "SILVA-LSU", "UNITE", "ITSoneDB", "PR2"]
        if asv_folder.exists():
            if (
                sum(
                    [
                        Path(f"{taxonomy_summary_folder}/{db}").exists()
                        for db in dada2_tax_names
                    ]
                )
                != 2
            ):
                reason = f"missing one of DADA2 tax folders in {EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}"
        else:
            if (
                sum(
                    [
                        Path(f"{taxonomy_summary_folder}/{db}").exists()
                        for db in dada2_tax_names
                    ]
                )
                != 0
            ):
                reason = f"DADA2 db exists but no {EMG_CONFIG.amplicon_pipeline.asv_folder} found"
        for db in taxonomy_summary_folder.iterdir():
            if db.name in tax_dbs:
                html = Path(f"{db}/{run_id}.html")
                mseq = Path(f"{db}/{run_id}_{db.name}.mseq")
                tsv = Path(f"{db}/{run_id}_{db.name}.tsv")
                txt = Path(f"{db}/{run_id}_{db.name}.txt")
                if not (
                    html.exists() and mseq.exists() and tsv.exists() and txt.exists()
                ):
                    reason = f"missing file in {db}"
            elif db.name in dada2_tax_names and asv_folder.exists():
                if not Path(f"{db}/{run_id}_{db.name}.mseq").exists():
                    reason = f"missing mseq in {db}"
                else:
                    for region in amplified_regions:
                        region_krona = Path(
                            f"{db}/{run_id}_{region}_{db.name}_asv_krona_counts.txt"
                        )
                        region_html = Path(f"{db}/{run_id}_{region}.html")
                        if not (region_html.exists() and region_krona.exists()):
                            reason = f"missing {region} file in {db}"
                    # checking concat folder
                    if len(amplified_regions) == 2:
                        concat_html = Path(f"{db}/{run_id}_concat.html")
                        concat_krona = Path(
                            f"{db}/{run_id}_concat_{db.name}_asv_krona_counts.txt"
                        )
                        if not (concat_krona.exists() and concat_html.exists()):
                            reason = f"missing concat files in {db}"
            else:
                reason = f"unknown {db} in {EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}"

    # QC mandatory folder
    if qc_folder.exists():
        if not Path(f"{qc_folder}/{analysis.run.first_accession}_seqfu.tsv").exists():
            reason = f"No required seqfu.tsv in {EMG_CONFIG.amplicon_pipeline.qc_folder} folder"
    else:
        reason = f"No {EMG_CONFIG.amplicon_pipeline.qc_folder} folder"
    logger.info(f"Post sanity check for {run_id}: {reason}")

    if reason:
        task_mark_analysis_status(
            analysis,
            status=AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED,
            reason=reason,
        )


@task(log_prints=True)
def import_completed_analysis(
    amplicon_current_outdir: Path, amplicon_analyses: List[analyses.models.Analysis]
):
    for analysis in amplicon_analyses:
        analysis.refresh_from_db()
        if not analysis.status.get(AnalysisStates.ANALYSIS_COMPLETED):
            print(f"{analysis} is not completed successfuly. Skipping.")
            continue
        if analysis.annotations.get(analysis.TAXONOMIES):
            print(f"{analysis} already has taxonomic annotations. Skipping.")
            continue

        dir_for_analysis = amplicon_current_outdir / analysis.run.first_accession

        analysis.results_dir = str(dir_for_analysis)
        analysis.save()

        import_qc(analysis, dir_for_analysis)

        for source in analyses.models.Analysis.TaxonomySources:
            if source in [
                analyses.models.Analysis.TaxonomySources.DADA2_PR2,
                analyses.models.Analysis.TaxonomySources.DADA2_SILVA,
            ]:
                print(f"IGNORING RESULTS FROM {source}! Not implemented yet.")
                # TODO: handle variable region naming conventions...
                continue
            import_taxonomy(
                analysis, dir_for_analysis, source=source, allow_non_exist=True
            )


@task(
    cache_key_fn=context_agnostic_task_input_hash,
)
def set_post_analysis_states(amplicon_current_outdir: Path, amplicon_analyses: List):
    # The pipeline produces top level end of execution reports, which contain
    # the list of the runs that were completed, and those that were not.
    # For more information: https://github.com/EBI-Metagenomics/amplicon-pipeline?tab=readme-ov-file#top-level-reports

    # qc_failed_runs.csv: runID,reason(seqfu_fail/sfxhd_fail/libstrat_fail/no_reads)
    qc_failed_csv = Path(
        f"{amplicon_current_outdir}/{EMG_CONFIG.amplicon_pipeline.failed_runs_csv}"
    )
    qc_failed_runs = {}  # Stores {run_accession, qc_fail_reason}

    if qc_failed_csv.is_file():
        with qc_failed_csv.open(mode="r") as file_handle:
            for row in csv.reader(file_handle, delimiter=","):
                run_accession, fail_reason = row
                qc_failed_runs[run_accession] = fail_reason

    # qc_passed_runs.csv: runID,info(all_results/no_asvs)
    qc_completed_csv = Path(
        f"{amplicon_current_outdir}/{EMG_CONFIG.amplicon_pipeline.completed_runs_csv}"
    )
    qc_completed_runs = {}  # Stores {run_accession, qc_fail_reason}

    if qc_completed_csv.is_file():
        with qc_completed_csv.open(mode="r") as file_handle:
            for row in csv.reader(file_handle, delimiter=","):
                run_accession, info = row
                qc_completed_runs[run_accession] = info

    for analysis in amplicon_analyses:
        if analysis.run.first_accession in qc_failed_runs:
            task_mark_analysis_status(
                analysis,
                status=AnalysisStates.ANALYSIS_QC_FAILED,
                reason=qc_failed_runs[analysis.run.first_accession],
            )
        elif analysis.run.first_accession in qc_completed_runs:
            task_mark_analysis_status(
                analysis,
                status=AnalysisStates.ANALYSIS_COMPLETED,
                reason=qc_completed_runs[analysis.run.first_accession],
                unset_statuses=[
                    AnalysisStates.ANALYSIS_FAILED,
                    AnalysisStates.ANALYSIS_BLOCKED,
                ],
            )
            sanity_check_amplicon_results(
                Path(f"{amplicon_current_outdir}/{analysis.run.first_accession}"),
                analysis,
            )
        else:
            task_mark_analysis_status(
                analysis,
                status=AnalysisStates.ANALYSIS_FAILED,
                reason="Missing run in execution",
            )


@flow(name="Run analysis pipeline-v6 in parallel", log_prints=True)
async def perform_amplicons_in_parallel(
    mgnify_study: analyses.models.Study,
    amplicon_analysis_ids: List[Union[str, int]],
):
    amplicon_analyses = analyses.models.Analysis.objects.select_related("run").filter(
        id__in=amplicon_analysis_ids,
        run__metadata__fastq_ftps__isnull=False,
    )
    samplesheet, ss_hash = make_samplesheet_amplicon(mgnify_study, amplicon_analyses)

    async for analysis in amplicon_analyses:
        mark_analysis_as_started(analysis)

    amplicon_current_outdir_parent = Path(
        f"{EMG_CONFIG.slurm.default_workdir}/{mgnify_study.ena_study.accession}_amplicon_v6"
    )

    amplicon_current_outdir = (
        amplicon_current_outdir_parent
        / ss_hash[:6]  # uses samplesheet hash prefix as dir name for the chunk
    )
    print(f"Using output dir {amplicon_current_outdir} for this execution")

    command = (
        f"nextflow run {EMG_CONFIG.amplicon_pipeline.amplicon_pipeline_repo} "
        f"-r {EMG_CONFIG.amplicon_pipeline.amplicon_pipeline_git_revision} "
        f"-latest "  # Pull changes from GitHub
        f"-profile {EMG_CONFIG.amplicon_pipeline.amplicon_pipeline_nf_profile} "
        f"-resume "
        f"--input {samplesheet} "
        f"--outdir {amplicon_current_outdir} "
        f"{'-with-tower' if settings.EMG_CONFIG.slurm.use_nextflow_tower else ''} "
        f"-name amplicon-v6-for-samplesheet-{slugify(samplesheet)[-30:]} "
    )

    try:
        env_variables = (
            "ALL,TOWER_WORKSPACE_ID"
            + f"{',TOWER_ACCESS_TOKEN' if settings.EMG_CONFIG.slurm.use_nextflow_tower else ''} "
        )
        run_cluster_job(
            name=f"Analyse amplicon study {mgnify_study.ena_study.accession} via samplesheet {slugify(samplesheet)}",
            command=command,
            expected_time=timedelta(
                days=EMG_CONFIG.amplicon_pipeline.amplicon_pipeline_time_limit_days
            ),
            memory=f"{EMG_CONFIG.amplicon_pipeline.amplicon_nextflow_master_job_memory_gb}G",
            environment=env_variables,
            input_files_to_hash=[samplesheet],
            working_dir=amplicon_current_outdir,
            resubmit_policy=ResubmitIfFailedPolicy,
        )
    except ClusterJobFailedException:
        for analysis in amplicon_analyses:
            mark_analysis_as_failed(analysis)
    else:
        # assume that if job finished, all finished... set statuses
        set_post_analysis_states(amplicon_current_outdir, amplicon_analyses)
        import_completed_analysis(amplicon_current_outdir, amplicon_analyses)


@flow(
    name="Run analysis pipeline-v6 on amplicon study",
    log_prints=True,
    flow_run_name="Analyse amplicon: {study_accession}",
    task_runner=SequentialTaskRunner,
)
async def analysis_amplicon_study(study_accession: str):
    """
    Get a study from ENA, and input it to MGnify.
    Kick off amplicon-v6 pipeline.
    :param study_accession: Study accession e.g. PRJxxxxxx
    """
    logger = get_run_logger()
    # Create/get ENA Study object
    ena_study = await ena.models.Study.objects.get_ena_study(study_accession)
    if not ena_study:
        ena_study = await get_study_from_ena(study_accession)
        await ena_study.arefresh_from_db()
    logger.info(f"ENA Study is {ena_study.accession}: {ena_study.title}")

    # Get a MGnify Study object for this ENA Study
    mgnify_study = await analyses.models.Study.objects.get_or_create_for_ena_study(
        study_accession
    )
    await mgnify_study.arefresh_from_db()
    logger.info(f"MGnify study is {mgnify_study.accession}: {mgnify_study.title}")

    read_runs = get_study_readruns_from_ena(
        ena_study.accession,
        limit=10000,
        filter_library_strategy=EMG_CONFIG.amplicon_pipeline.amplicon_library_strategy,
        extra_cache_hash=ena_study.fetched_at.isoformat(),  # if ENA study is deleted/updated, the cache should be invalidated
    )
    logger.info(f"Returned {len(read_runs)} run from ENA portal API")

    # get or create Analysis for runs
    mgnify_analyses = create_analyses(
        mgnify_study,
        for_experiment_type=analyses.models.WithExperimentTypeModel.ExperimentTypes.AMPLICON,
        pipeline=analyses.models.Analysis.PipelineVersions.v6,
    )
    analyses_to_attempt = get_analyses_to_attempt(
        mgnify_study,
        for_experiment_type=analyses.models.WithExperimentTypeModel.ExperimentTypes.AMPLICON,
    )

    # Work on chunks of 20 readruns at a time
    # Doing so means we don't use our entire cluster allocation for this study
    chunked_runs = chunk_list(
        analyses_to_attempt, EMG_CONFIG.amplicon_pipeline.samplesheet_chunk_size
    )
    for analyses_chunk in chunked_runs:
        # launch jobs for all analyses in this chunk in a single flow
        logger.info(
            f"Working on amplicon analyses: {analyses_chunk[0]}-{analyses_chunk[-1]}"
        )
        await perform_amplicons_in_parallel(mgnify_study, analyses_chunk)
