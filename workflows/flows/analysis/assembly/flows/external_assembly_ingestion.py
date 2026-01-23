import csv
from collections import defaultdict
from pathlib import Path
from textwrap import dedent as _
from typing import Optional, List

from prefect import flow, get_run_logger, suspend_flow_run, task
from prefect.input import RunInput
from pydantic import Field

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
)
from workflows.flows.analyse_study_tasks.shared.study_summary import (
    merge_assembly_study_summaries,
)
from workflows.flows.analysis.assembly.tasks.add_assembly_study_summaries_to_downloads import (
    add_assembly_study_summaries_to_downloads,
)
from workflows.prefect_utils.analyses_models_helpers import (
    get_users_as_choices,
    add_study_watchers,
)
from workflows.models import AssemblyAnalysisPipeline, Analysis


@flow(
    name="Ingest external assembly analysis results",
    flow_run_name="Ingest assembly results: {results_dir}",
)
def external_assembly_ingestion(
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

    if not results_path.exists():
        raise FileNotFoundError(f"Results directory does not exist: {results_dir}")

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
    study2assemblies_map = defaultdict(list)
    for assembly_accession in assembly_accessions:
        study_accession = get_study_accession_for_assembly(assembly_accession)
        study2assemblies_map[study_accession].append(assembly_accession)

    # Process each study accession and its assemblies
    for study_accession, assembly_accessions in study2assemblies_map.items():
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

        # Set results_dir to the provided external path
        logger.info(f"Set results_dir to {results_path}")
        mgnify_study.results_dir = str(results_path)
        mgnify_study.save()

        # Check if biome-picker is needed
        # TODO: think about how to avoid setting biome for 200 times if samplesheet has many studies
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
            logger.info(
                f"MGnify study {mgnify_study.accession} has biome {biome.path}."
            )

            if ingest_input.watchers:
                add_study_watchers(mgnify_study, ingest_input.watchers)
        else:
            logger.info(
                f"Biome {mgnify_study.biome} was already set for this study. If an change is needed, do so in the DB Admin Panel."
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
            results_path / "asa",
            exported_analyses,
        )

        logger.info("Processing ASA results...")
        process_external_pipeline_results(
            exported_analyses,
            results_path,
            AssemblyAnalysisPipeline.ASA,
        )

        logger.info("Processing VIRify results...")
        process_external_pipeline_results(
            exported_analyses,
            results_path,
            AssemblyAnalysisPipeline.VIRIFY,
        )

        logger.info("Processing MAP results...")
        process_external_pipeline_results(
            exported_analyses,
            results_path,
            AssemblyAnalysisPipeline.MAP,
        )

        # =========================================================================
        # STEP 6: Copy results files to production locations
        # =========================================================================
        logger.info("Step 6: Copying results to production locations...")
        _copy_results_files(mgnify_study, results_path)
        # TODO maybe copy_v6_pipeline_results flow can be reused here?

        # =========================================================================
        # STEP 7: Finalize study and create summaries
        # =========================================================================
        logger.info("Step 7: Finalizing study and creating summaries...")
        # TODO add step to generate study summaries

        merge_assembly_study_summaries(
            mgnify_study.accession,
            cleanup_partials=True,
        )
        logger.info("Merged assembly study summaries")

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

        logger.info(f"Ingestion complete for study {mgnify_study.accession}")


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================


def _parse_and_validate_samplesheet(samplesheet_path: Path) -> list[str]:
    """
    Parse the samplesheet CSV to extract assembly accessions.

    Expected samplesheet format (CSV with header):
        sample,assembly_fasta,contaminant_reference,human_reference,phix_reference
        ERZ18440741,/path/to/ERZ18440741.fa.gz,<contaminant_ref>,<human_ref>,<phix_ref>
        ERZ18545484,/path/to/ERZ18545484.fa.gz,<contaminant_ref>,<human_ref>,<phix_ref>

    The "sample" column contains the assembly accession (e.g., ERZ format).

    :param samplesheet_path: Path to the samplesheet file
    :return: List of assembly accessions extracted from the samplesheet
    :raises: ValueError if samplesheet format is invalid
    """
    logger = get_run_logger()
    assembly_accessions = []

    # TODO add validation with pandera model
    # TODO add validation of the fasta file using md5 checksums from ENA website
    try:
        with open(samplesheet_path, "r") as f:
            reader = csv.DictReader(f)

            if reader.fieldnames is None:
                raise ValueError("Samplesheet appears to be empty")

            expected_columns = {
                "sample",
                "assembly_fasta",
                "contaminant_reference",
                "human_reference",
                "phix_reference",
            }
            if not expected_columns.issubset(set(reader.fieldnames)):
                logger.warning(
                    f"Samplesheet columns {set(reader.fieldnames)} may not match expected "
                    f"columns {expected_columns}"
                )

            for row in reader:
                assembly_acc = row.get("sample", "").strip()
                if assembly_acc:
                    assembly_accessions.append(assembly_acc)
                    logger.debug(f"Parsed assembly accession: {assembly_acc}")

    except Exception as e:
        raise ValueError(f"Error parsing samplesheet {samplesheet_path}: {e}")

    if not assembly_accessions:
        raise ValueError(
            f"No assembly accessions found in samplesheet {samplesheet_path}"
        )

    return assembly_accessions


# TODO: expand validation checks
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
    - Expected assembly accessions in output files

    :param results_path: Path to results directory
    :param assemblies_accessions: List of expected assembly accessions
    :raises: ValueError if critical validation fails
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
            logger.warning(
                f"Pipeline directory {pipeline} not found at {pipeline_path}. "
                f"Validation will proceed, but {pipeline} results will not be imported."
            )
        else:
            logger.info(f"Found {pipeline} results directory: {pipeline_path}")


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


def _copy_results_files(
    mgnify_study: analyses.models.Study, results_path: Path
) -> None:
    """
    Copy pipeline results to production locations.

    Copies results to:
    - /nfs/prod/ for production results
    - /nfs/services/ for service files (public downloads)

    :param mgnify_study: The MGnify Study object
    :param results_path: Path to source results directory
    """
    logger = get_run_logger()

    # TODO: Implement file copying logic
    # This should:
    # 1. Identify which files need to be copied based on pipeline types
    # 2. Copy ASA results to /nfs/prod/{study_accession}/asa/
    # 3. Copy VIRify results to /nfs/prod/{study_accession}/virify/
    # 4. Copy MAP results to /nfs/prod/{study_accession}/map/
    # 5. Copy public files to /nfs/services/
    #
    # Reuse utilities from workflows/data_io_utils/ if available

    logger.info(f"Copying results files for {mgnify_study.accession}...")
    logger.info(f"Source: {results_path}")
    logger.info("File copying not yet implemented (placeholder)")


@task
def set_asa_analysis_states(assembly_current_outdir: Path, analyses: List[Analysis]):
    """
    This function processes the end-of-execution reports generated by the assembly analysis pipeline
    to determine the outcome of each assembly and updates their status accordingly.

    TODO: this function is a copy-paste of set_post_assembly_analysis_states with some changes, needs refactoring.

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
            logger.error(f"QC failed - {analysis}")
            analysis.mark_status(
                AnalysisStates.ANALYSIS_QC_FAILED,
                set_status_as=True,
                reason=qc_failed_assemblies[assembly_accession],
                save=False,
            )
            analyses_to_update.append(analysis)
        else:
            logger.error(f"Assembly {analysis} missing from CSV.")
            analysis.mark_status(
                AnalysisStates.ANALYSIS_QC_FAILED,
                set_status_as=True,
                save=False,
                reason=f"Assembly missing from {analysed_assemblies_csv}.",
            )
            analyses_to_update.append(analysis)

    # Bulk update - slightly gentler on the DB
    Analysis.objects.bulk_update(analyses_to_update, ["status"])
