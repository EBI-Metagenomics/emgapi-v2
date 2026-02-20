import csv
import hashlib
from pathlib import Path
from textwrap import dedent as _
from typing import Optional, List

from prefect import flow, get_run_logger, suspend_flow_run, task
from prefect.input import RunInput
from pydantic import Field
from django.conf import settings

import analyses.models
import ena.models
from workflows.data_io_utils.schemas import (
    AssemblyResultSchema,
    VirifyResultSchema,
    MapResultSchema,
)
from workflows.ena_utils.ena_api_requests import (
    get_study_from_ena,
    get_study_assemblies_from_ena,
    get_study_accession_for_assembly,
    get_fasta_md5_for_assembly,
)
from workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer import (
    validate_and_import_analysis_results,
)
from workflows.flows.analysis.assembly.tasks.create_analyses_for_assemblies import (
    create_analyses_for_assemblies,
)
from workflows.flows.analysis.assembly.tasks.process_import_results import (
    mark_analyses_with_failed_status,
)
from workflows.flows.analyse_study_tasks.shared.analysis_states import AnalysisStates
from workflows.flows.assemble_study import get_biomes_as_choices
from workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results import (
    copy_v6_study_summaries,
    _copy_external_single_analysis_results,
)
from workflows.flows.analysis.assembly.tasks.assembly_analysis_pipeline_batch_study_summary_generator import (
    generate_assembly_analysis_pipeline_summary,
)
from workflows.flows.analysis.assembly.tasks.add_assembly_study_summaries_to_downloads import (
    add_assembly_study_summaries_to_downloads,
)
from workflows.prefect_utils.analyses_models_helpers import (
    get_users_as_choices,
    add_study_watchers,
)
from workflows.models import AssemblyAnalysisPipeline, Analysis, Study


@flow(
    name="Ingest external assembly analysis results",
    flow_run_name="Ingest assembly results: {results_dir}",
)
def external_assembly_analysis_ingestion(
    results_dir: str,
    samplesheet_path: str,
):
    """
    Ingest Assembly Analysis Pipeline results that were run externally.

    This flow accepts a path to externally-run ASA/VIRify/MAP results and:
    1. Creates/fetches the required database models (study, assemblies and analyses)
    2. Validates that the results match the samplesheet and filesystem structure
    3. Imports the pipeline results into the production system
    4. Triggers study summary creation and file copying

    :param results_dir: Path to the directory containing externally-run results.
                        Should contain folders: asa/, virify/, map/
    :param samplesheet_path: Path to the samplesheet CSV used for the external run.
    """
    logger = get_run_logger()
    results_path = Path(results_dir)
    samplesheet_path = Path(samplesheet_path)

    if not results_path.exists():
        raise FileNotFoundError(f"Results directory does not exist: {results_dir}")

    if not samplesheet_path.is_file():
        raise FileNotFoundError(f"Samplesheet file does not exist: {samplesheet_path}")

    logger.info(f"Starting external assembly ingestion from {results_dir}")

    # =========================================================================
    # STEP 1: Parse samplesheet structure and extract assembly accessions
    # =========================================================================
    logger.info(
        "Step 1: Parsing samplesheet structure and extracting assembly accessions..."
    )

    assembly_accessions = _parse_and_validate_samplesheet(samplesheet_path)
    logger.info(
        f"Extracted {len(assembly_accessions)} assembly accessions from samplesheet"
    )
    for acc in assembly_accessions:
        logger.info(f"  - {acc}")

    # =========================================================================
    # STEP 2: Validate filesystem and results structure
    # =========================================================================
    logger.info("Step 2: Validating results structure and consistency...")
    _validate_results_structure(results_path, assembly_accessions)

    # =========================================================================
    # STEP 3: Create/fetch Study, Assemblies and related objects, set Biome
    # =========================================================================
    logger.info("Step 3: Creating/fetching study and related models...")

    # Find study accession for each assembly accession in the samplesheet
    study_accessions = set()
    for assembly_accession in assembly_accessions:
        study_accession = get_study_accession_for_assembly(assembly_accession)
        study_accessions.add(study_accession)

    if len(study_accessions) > 1:
        raise ValueError(
            f"Samplesheet contains assemblies from multiple studies: {study_accessions}. "
            "Please provide a samplesheet with assemblies from a single study."
        )

    study_accession = study_accessions.pop()

    # Process the study and its assemblies
    logger.info(f"Processing study accession: {study_accession}")
    ena_study = ena.models.Study.objects.get_ena_study(study_accession)
    if not ena_study:
        ena_study = get_study_from_ena(study_accession)

    ena_study.refresh_from_db()
    logger.info(f"ENA Study is {ena_study.accession}: {ena_study.title}")

    mgnify_study = analyses.models.Study.objects.get_or_create_for_ena_study(
        study_accession
    )
    mgnify_study.refresh_from_db()
    logger.info(f"MGnify study is {mgnify_study.accession}: {mgnify_study.title}")

    # Run this function to fetch related runs/samples for this study and save them in the DB
    get_study_assemblies_from_ena(ena_study.accession, limit=10000)

    # TODO check if this code makes sense in this context
    # Set results_dir to the provided external path
    logger.info(f"Set results_dir to {results_path}")
    mgnify_study.results_dir = str(results_path)
    mgnify_study.save()

    # Check if biome-picker is needed
    if not mgnify_study.biome:
        BiomeChoices = get_biomes_as_choices()
        UserChoices = get_users_as_choices()

        class IngestExternalInput(RunInput):
            biome: BiomeChoices
            watchers: Optional[List[UserChoices]] = Field(
                None,
                description="Admin users watching this study will get status notifications.",
            )

        ingest_input: IngestExternalInput = suspend_flow_run(
            wait_for_input=IngestExternalInput.with_initial_data(
                description=_(
                    f"""\
                        **External Assembly Analysis Ingestion**
                        Ingesting externally-run Assembly Analysis results for study {ena_study.accession} \
                        produced by [Assembly Analysis Pipeline V6](https://github.com/EBI-Metagenomics/assembly-analysis-pipeline).

                        **Biome tagger**
                        Please select a Biome for the entire study \
                        [{ena_study.accession}: {ena_study.title}](https://www.ebi.ac.uk/ena/browser/view/{ena_study.accession}).
                        """
                ),
            )
        )

        biome = analyses.models.Biome.objects.get(path=ingest_input.biome.name)
        mgnify_study.biome = biome
        mgnify_study.save()
        logger.info(f"MGnify study {mgnify_study.accession} has biome {biome.path}.")

        if ingest_input.watchers:
            add_study_watchers(mgnify_study, ingest_input.watchers)
    else:
        logger.info(
            f"Biome {mgnify_study.biome} was already set for this study. If a change is needed, do so in the DB Admin Panel."
        )
        logger.info(
            f"MGnify study has watchers: {mgnify_study.watchers.values_list('username', flat=True)}. Add more in the DB Admin Panel if needed."
        )

    # =========================================================================
    # STEP 4: Create Analysis objects
    # =========================================================================
    logger.info("Step 4: Creating Analysis objects for all assemblies...")
    exported_analyses = create_analyses_for_assemblies(
        mgnify_study.id,
        assembly_accessions,
        pipeline=analyses.models.Analysis.PipelineVersions.v6,
    )

    # =========================================================================
    # STEP 5: Validate and Import Pipeline Results
    # =========================================================================
    logger.info("Step 5: Validating and importing pipeline results...")

    logger.info("Setting ASA analysis states...")
    set_asa_analysis_states(
        results_path / "asa",  # may as well be mgnify_study.results_dir / "asa"
        exported_analyses,
    )

    logger.info("Processing ASA results...")
    process_external_pipeline_results(
        exported_analyses,
        results_path,
        AssemblyAnalysisPipeline.ASA,
    )

    logger.info("Processing VIRify results...")
    # TODO in production we only import if VIRIFY statuses are present, do we want same here?
    process_external_pipeline_results(
        exported_analyses,
        results_path,
        AssemblyAnalysisPipeline.VIRIFY,
    )

    logger.info("Processing MAP results...")
    # TODO in production we only import if MAP statuses are present, do we want same here?
    process_external_pipeline_results(
        exported_analyses,
        results_path,
        AssemblyAnalysisPipeline.MAP,
    )

    # =========================================================================
    # STEP 6: Finalize study and create summaries
    # =========================================================================
    logger.info("Step 6: Finalizing study and creating summaries...")

    generate_assembly_analysis_pipeline_summary(
        study=mgnify_study,
    )

    # TODO: if more analysis will be analyses in future, how to update summaries?

    add_assembly_study_summaries_to_downloads(mgnify_study.accession)
    logger.info("Added study summaries to downloads")

    copy_v6_study_summaries(mgnify_study.accession)
    logger.info("Copied v6 study summaries")

    # Update study features
    mgnify_study.refresh_from_db()
    mgnify_study.features.has_v6_analyses = mgnify_study.analyses.filter(
        pipeline_version=analyses.models.Analysis.PipelineVersions.v6, is_ready=True
    ).exists()
    mgnify_study.save()

    # =========================================================================
    # STEP 7: Copy results files to production locations
    # =========================================================================
    logger.info("Step 7: Copying results to production locations...")
    copy_external_assembly_analysis_results(
        study=mgnify_study,
        results_dir=results_path,
        analyses=exported_analyses,
        timeout=14400,
    )

    logger.info(f"Ingestion complete for study {mgnify_study.accession}")


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


@task
def _parse_and_validate_samplesheet(samplesheet_path: Path) -> list[str]:
    """
    Parse the samplesheet CSV to extract assembly accessions, validate required columns
    and check that the provided FASTA files match the expected MD5 checksums from ENA.

    Expected samplesheet format (CSV with header):
        sample,assembly_fasta,contaminant_reference,human_reference,phix_reference
        ERZ18440741,/path/to/ERZ18440741.fa.gz,<contaminant_ref>,<human_ref>,<phix_ref>
        ERZ18545484,/path/to/ERZ18545484.fa.gz,<contaminant_ref>,<human_ref>,<phix_ref>

    The "sample" column contains the assembly accession (e.g., ERZ format).
    "sample" and "assembly_fasta" columns are required, others are optional.

    :param samplesheet_path: Path to the samplesheet file
    :return: List of assembly accessions extracted from the samplesheet
    :raises: ValueError if samplesheet format is invalid
    """
    logger = get_run_logger()
    assembly_accessions = []

    # TODO maybe add validation with pandera model?
    try:
        with open(samplesheet_path, "r") as f:
            reader = csv.DictReader(f)

            if reader.fieldnames is None:
                raise ValueError("Samplesheet appears to be empty")

            actual_columns = set(reader.fieldnames)

            optional_columns = {
                "contaminant_reference",
                "human_reference",
                "phix_reference",
            }

            required_columns = {"sample", "assembly_fasta"}

            if not required_columns.issubset(actual_columns):
                raise ValueError(
                    f"Samplesheet is missing required columns: {required_columns - actual_columns}"
                )

            if not actual_columns.issubset(optional_columns | required_columns):
                raise ValueError(
                    f"Samplesheet contains unexpected columns: {actual_columns - (optional_columns | required_columns)}. "
                )

            for row in reader:
                assembly_acc = row.get("sample", "").strip()
                if assembly_acc:
                    fasta_file = Path(row.get("assembly_fasta", "").strip())
                    actual_fasta_md5 = _compute_md5(fasta_file)
                    expected_fasta_md5 = get_fasta_md5_for_assembly(assembly_acc)

                    if actual_fasta_md5 != expected_fasta_md5:
                        raise ValueError(
                            f"FASTA MD5 mismatch for assembly {assembly_acc}: "
                            f"expected {expected_fasta_md5} (ENA generated_md5 field), got {actual_fasta_md5}. "
                            f"Check that the correct FASTA file is provided in the samplesheet."
                        )
                    logger.debug(
                        f"FASTA MD5 for assembly {assembly_acc} matches expected value"
                    )

                    assembly_accessions.append(assembly_acc)
                    logger.debug(f"Parsed assembly accession: {assembly_acc}")

    except Exception as e:
        raise ValueError(f"Error parsing samplesheet {samplesheet_path}: {e}")

    if not assembly_accessions:
        raise ValueError(
            f"No assembly accessions found in samplesheet {samplesheet_path}"
        )

    return assembly_accessions


def _compute_md5(path: Path, chunk_size: int = 8192) -> str:
    md5 = hashlib.md5()

    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            md5.update(chunk)

    return md5.hexdigest()


@task
def _validate_results_structure(
    results_path: Path,
    assemblies_accessions: list[str],
) -> None:
    """
    Validate that the results directory structure matches expected format.

    Checks for:
    - ASA output subdirectory
    - VIRify output subdirectory
    - MAP output subdirectory
    - Subdirectories for each assembly accession within each pipeline's output directory

    :param results_path: Path to results directory
    :param assemblies_accessions: List of expected assembly accessions
    :raises: FileNotFoundError if critical validation fails
    """
    logger = get_run_logger()

    logger.info("Validating results structure...")
    logger.info(f"Expected {len(assemblies_accessions)} assemblies")

    # Check for expected subdirectories
    expected_dirs = {
        "asa": results_path / "asa",
        "virify": results_path / "virify",
        "map": results_path / "map",
    }

    for pipeline, pipeline_path in expected_dirs.items():
        if not pipeline_path.exists():
            raise FileNotFoundError(
                f"Pipeline directory {pipeline} not found at {pipeline_path}."
            )
        else:
            logger.info(f"Found {pipeline} results directory: {pipeline_path}")
        for assembly_acc in assemblies_accessions:
            assembly_results_folder = pipeline_path / assembly_acc
            if not assembly_results_folder.exists():
                raise FileNotFoundError(
                    f"Results folder {assembly_results_folder} not found."
                )


@flow(
    flow_run_name="Import externally generated analysis results: {pipeline_type}",
    retries=2,
    retry_delay_seconds=60,
)
def process_external_pipeline_results(
    analyses: list[Analysis],
    results_path: Path,
    pipeline_type: AssemblyAnalysisPipeline,
) -> None:
    """Import pipeline results without using batch abstraction."""
    logger = get_run_logger()

    schema = None
    base_path = None

    # This is a bit ugly, but it is done like this to avoid having Prefect to serialize a complex schema as input
    if pipeline_type == AssemblyAnalysisPipeline.ASA:
        schema = AssemblyResultSchema()
        base_path = results_path / "asa"
    elif pipeline_type == AssemblyAnalysisPipeline.VIRIFY:
        schema = VirifyResultSchema()
        base_path = results_path / "virify"
    elif pipeline_type == AssemblyAnalysisPipeline.MAP:
        schema = MapResultSchema()
        base_path = results_path / "map"

    if schema is None or base_path is None:
        raise ValueError(f"Unsupported pipeline type: {pipeline_type}")
    if not base_path.exists():
        raise FileNotFoundError(
            f"Results directory for the {pipeline_type} does not exist: {base_path}"
        )

    # First, validate schemas (without importing)
    logger.info(f"Validating {pipeline_type} results schemas...")
    validation_results = validate_and_import_analysis_results(
        analyses, schema, base_path, pipeline_type, validation_only=True
    )

    # Process validation results - mark failures as FAILED
    # TODO: check with Martin if we want to mark virify/map analyses as QC_FAILED if no results found
    mark_analyses_with_failed_status(
        validation_results,
        pipeline_type,
        validation_only=True,
    )

    # Only import if validation passed for at least one analysis
    successful_validations = [r for r in validation_results if r.success]
    if successful_validations:
        logger.info(
            f"Importing {len(successful_validations)} validated {pipeline_type} analyses..."
        )
        import_results = validate_and_import_analysis_results(
            analyses, schema, base_path, pipeline_type, validation_only=False
        )
        # Process import results (should all succeed since validation passed)
        mark_analyses_with_failed_status(
            import_results,
            pipeline_type,
            validation_only=False,
        )
    else:
        logger.error(f"No {pipeline_type} analyses passed the results validation.")


@task
def set_asa_analysis_states(assembly_current_outdir: Path, analyses: List[Analysis]):
    """
    This function processes the end-of-execution reports generated by the assembly analysis pipeline
    to determine the outcome of each assembly and updates their status accordingly.

    TODO: this function is a copy-paste of set_post_assembly_analysis_states with some changes,
    needs refactoring (ochkalova).

    The pipeline produces two CSV files:
    - qc_failed_assemblies.csv: Lists assemblies that failed QC checks (assemblyID, reason)
    - analysed_assemblies.csv: Lists assemblies that completed successfully (assemblyID, info)

    Logic flow:
    1. Read QC failures from qc_failed_assemblies.csv if it exists
    2. Read successful analyses from analysed_assemblies.csv (raises error if the file is missing)
    3. For each assembly analysis:
    - If assembly is in QC failures: Mark workflow_status as FAILED and set status to ANALYSIS_QC_FAILED
    - If assembly is in successful analyses: Mark workflow_status as COMPLETED
    - If assembly is in neither list: Mark workflow_status as FAILED with unknown reason
    4. Bulk update all analyses to minimize database operations

    :param assembly_current_outdir: Path to the directory containing the pipeline output
    :param analyses: List of assembly analyses objects to process
    :raises ValueError: If the analysed_assemblies.csv file is missing (catastrophic failure)
    """
    # The pipeline produces top-level end-of-execution reports, which contain
    # the list of the assemblies that were completed and those that were not.
    # Similar to the amplicon pipeline

    logger = get_run_logger()

    # qc_failed_assemblies.csv: assemblyID,reason
    qc_failed_csv = Path(f"{assembly_current_outdir}/qc_failed_assemblies.csv")
    qc_failed_assemblies = {}  # Stores {assembly_accession, qc_fail_reason}

    if qc_failed_csv.is_file():
        logger.info("Reading qc failed assemblies...")
        with qc_failed_csv.open(mode="r") as file_handle:
            for row in csv.reader(file_handle, delimiter=","):
                assembly_accession, fail_reason = row
                qc_failed_assemblies[assembly_accession] = fail_reason

    # analysed_assemblies.csv: assemblyID, info
    analysed_assemblies_csv = Path(f"{assembly_current_outdir}/analysed_assemblies.csv")
    analysed_assemblies = {}  # Stores {assembly_accession, info}

    if analysed_assemblies_csv.is_file():
        with analysed_assemblies_csv.open(mode="r") as file_handle:
            for row in csv.reader(file_handle, delimiter=","):
                assembly_accession, info = row
                analysed_assemblies[assembly_accession] = info
    else:
        # The caller is responsible for handling this error -- which is quite catastrophic.
        raise ValueError(
            f"The end of run execution CSV file is missing. Expected path: {analysed_assemblies_csv}"
        )

    analyses_to_update = []

    for analysis in analyses:
        assembly_accession = analysis.assembly.first_accession

        if assembly_accession in qc_failed_assemblies:
            logger.error(f"QC failed - {assembly_accession}")
            analysis.mark_status(
                AnalysisStates.ANALYSIS_QC_FAILED,
                set_status_as=True,
                reason=qc_failed_assemblies[assembly_accession],
                save=False,
            )
            analyses_to_update.append(analysis)
        elif assembly_accession in analysed_assemblies:
            logger.info(
                f"{assembly_accession} marked as analyzed in the end of run CSV."
            )
        else:
            logger.error(f"Assembly {assembly_accession} missing from CSV.")
            analysis.mark_status(
                AnalysisStates.ANALYSIS_QC_FAILED,
                set_status_as=True,
                save=False,
                reason=f"Assembly missing from {analysed_assemblies_csv}.",
            )
            analyses_to_update.append(analysis)

    # Bulk update - slightly gentler on the DB
    Analysis.objects.bulk_update(analyses_to_update, ["status"])


@task
def copy_external_assembly_analysis_results(
    study: Study, results_dir: Path, analyses: List[Analysis], timeout: int = 14400
):
    """
    TODO: this function is a copy-paste of copy_assembly_batch_results with some changes,
    needs refactoring (ochkalova).

    Copy results from all three assembly analysis pipelines (ASA, VIRify, MAP)
    generated outside of production system for all analyses in the input list
    to destination directories with results.

    For each analysis in the batch, creates the following structure:
    - {target_root}/{study_path}/{assembly_accession}/ - ASA results at root
    - {target_root}/{study_path}/{assembly_accession}/virify/ - VIRify results
    - {target_root}/{study_path}/{assembly_accession}/mobilome-annotation/ - MAP results

    :param study: The study object to copy results for
    :param results_dir: The base path where the results are located
    :param analyses: The list of analyses objects to copy results for
    :param timeout: Timeout in seconds for each move operation (default: 4 hours)
    """
    logger = get_run_logger()

    # Determine target root based on privacy
    target_root = (
        settings.EMG_CONFIG.slurm.private_results_dir
        if study.is_private
        else settings.EMG_CONFIG.slurm.ftp_results_dir
    )

    # Only copy results for analyses where ASA completed successfully
    asa_completed_analyses = study.analyses.filter(
        pipeline_version=analyses.models.Analysis.PipelineVersions.v6,
        is_ready=True,  # TODO: check if I can use is_ready=True in this context
        id__in=[analysis.id for analysis in analyses],
    ).filter_by_statuses([AnalysisStates.ANALYSIS_COMPLETED])

    logger.info(
        f"Copying results for study {study.accession} ({asa_completed_analyses.count()} ASA-completed analyses "
        f"out of {len(analyses)} total) to {target_root}"
    )

    # Process each analysis that completed ASA
    copied_analysis_ids = []
    copy_failed_analysis_ids = []
    for analysis in asa_completed_analyses:
        try:
            # TODO: This is calls another flow, which feels too nested. We should review this (mbc)
            _copy_external_single_analysis_results(
                analysis=analysis,
                results_workspace=results_dir,
                target_root=target_root,
                logger=logger,
                timeout=timeout,
            )
        except Exception as e:
            copy_failed_analysis_ids.append(analysis.id)
            logger.error(f"Failed to copy results for {analysis.accession}: {e}")
            continue
        else:
            copied_analysis_ids.append(analysis.id)
    # Bulk update analysis statuses for all successfully copied analyses
    if copied_analysis_ids:
        analyses_to_update = Analysis.objects.filter(id__in=copied_analysis_ids)
        # Update status flags in bulk
        for analysis in analyses_to_update:
            analysis.status[Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED] = (
                True
            )
            # Unset failure/blocked statuses
            analysis.status[Analysis.AnalysisStates.ANALYSIS_QC_FAILED] = False
            analysis.status[
                Analysis.AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED
            ] = False
            analysis.status[Analysis.AnalysisStates.ANALYSIS_BLOCKED] = False
        Analysis.objects.bulk_update(analyses_to_update, ["status"])
        logger.info(f"Bulk updated status for {len(copied_analysis_ids)} analyses")

    if copy_failed_analysis_ids:
        analyses_to_update = Analysis.objects.filter(id__in=copy_failed_analysis_ids)
        # Update status flags in bulk
        for analysis in analyses_to_update:
            analysis.status[Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED] = (
                False
            )
        Analysis.objects.bulk_update(analyses_to_update, ["status"])
        logger.info(f"Bulk updated status for {len(copy_failed_analysis_ids)} analyses")

    logger.info(
        f"Completed copying results for study {study.ena_accessions.first_accession}"
    )
