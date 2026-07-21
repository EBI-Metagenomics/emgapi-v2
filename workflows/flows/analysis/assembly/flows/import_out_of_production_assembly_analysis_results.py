import csv
from pathlib import Path
from textwrap import dedent as _
from typing import List, Optional

from django.conf import settings
from prefect import flow, get_run_logger, suspend_flow_run, task
from prefect.input import RunInput
from pydantic import Field

from activate_django_first import EMG_CONFIG

import analyses.models
import ena.models
from workflows.data_io_utils.filenames import accession_prefix_separated_dir_path
from workflows.data_io_utils.schemas import (
    AssemblyResultSchema,
    MapResultSchema,
    VirifyResultSchema,
)
from workflows.ena_utils.ena_api_requests import (
    get_study_accession_for_assembly,
    get_study_assemblies_from_ena,
)
from workflows.ena_utils.webin_owner_utils import validate_and_set_webin_owner
from workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results import (
    BatchCopyResult,
    copy_single_analysis_results,
    copy_v6_study_summaries,
)
from workflows.flows.analysis.assembly.flows.sync_assembly_batch_results import (
    update_analysis_statuses_from_copy_results,
    update_external_results_dirs_from_copy_results,
    update_results_dirs_from_copy_results,
)
from workflows.flows.analysis.assembly.tasks.add_assembly_study_summaries_to_downloads import (
    add_assembly_study_summaries_to_downloads,
)
from workflows.flows.analysis.assembly.tasks.assembly_analysis_batch_results_importer import (
    assembly_analysis_results_importer,
)
from workflows.flows.analysis.assembly.tasks.assembly_analysis_pipeline_batch_study_summary_generator import (
    generate_assembly_analysis_pipeline_summary,
)
from workflows.flows.analysis.assembly.tasks.create_analyses_for_assemblies import (
    create_analyses_for_assemblies,
)
from workflows.flows.analysis.assembly.tasks.process_import_results import (
    mark_analyses_with_failed_status,
)
from workflows.flows.analysis.assembly.tasks.set_post_assembly_analysis_states import (
    parse_asa_end_of_run_reports,
    update_analyses_from_asa_reports,
)
from workflows.flows.assemble_study import get_biomes_as_choices
from workflows.flows.shared.study_tasks import get_or_create_mgnify_study
from workflows.models import Analysis, AssemblyAnalysisPipeline, Study
from workflows.prefect_utils.analyses_models_helpers import (
    add_study_watchers,
    get_users_as_choices,
)


@flow(
    name="Import out-of-production assembly analysis results",
    flow_run_name="Import out-of-production assembly analysis results: {results_dir}",
)
def import_out_of_production_assembly_analysis_results(
    results_dir: str,
    samplesheet_path: str,
):
    """
    Import Assembly Analysis Pipeline results that were run out-of-production.

    This flow takes samplesheet, a path to out-of-production ASA/VIRify/MAP results and:
    1. Creates/fetches the required database models (study, assemblies and analyses)
    2. Validates that the results match the samplesheet and expected folder structure
    3. Imports the pipeline results into the production system
    4. Triggers study summary creation and file copying

    :param results_dir: Path to the directory containing out-of-production results.
                        Should contain folders: asa/, virify/, map/
    :param samplesheet_path: Path to the samplesheet CSV used for the out-of-production run.
    """
    logger = get_run_logger()
    results_path = Path(results_dir)
    samplesheet_path = Path(samplesheet_path)

    if not results_path.exists():
        raise FileNotFoundError(f"Results directory does not exist: {results_dir}")

    if not samplesheet_path.is_file():
        raise FileNotFoundError(f"Samplesheet file does not exist: {samplesheet_path}")

    logger.info(
        f"Starting import of out-of-production assembly analysis results from {results_dir}"
    )

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
    mgnify_study_id = get_or_create_mgnify_study(study_accession)
    mgnify_study = analyses.models.Study.objects.get(id=mgnify_study_id)
    ena_study = ena.models.Study.objects.get_ena_study(study_accession)

    if mgnify_study.is_private:
        logger.info(f"{mgnify_study} is a private study.")

    # Run this function to fetch related runs/samples for this study and save them in the DB
    get_study_assemblies_from_ena(study_accession, limit=10000)

    # Set results_dir to the provided external path
    logger.info(f"Set results_dir to {results_path}")
    mgnify_study.results_dir = str(results_path)
    mgnify_study.save()

    # Suspend if biome is needed, or if the study is private and has no webin submitter set yet
    needs_biome = not mgnify_study.biome
    needs_webin = mgnify_study.is_private and not ena_study.webin_submitter

    if needs_biome or needs_webin:
        BiomeChoices = get_biomes_as_choices()
        UserChoices = get_users_as_choices()

        class ImportAssemblyAnalysisInput(RunInput):
            biome: BiomeChoices
            watchers: Optional[List[UserChoices]] = Field(
                None,
                description="Admin users watching this study will get status notifications.",
            )
            webin_owner: Optional[str] = Field(
                None,
                description="Webin ID of study owner, if data is private. Can be left as None if public.",
            )

        initial_data = {}
        if not needs_biome:
            initial_data["biome"] = BiomeChoices[str(mgnify_study.biome.path)]

        ingest_analysis_input: ImportAssemblyAnalysisInput = suspend_flow_run(
            wait_for_input=ImportAssemblyAnalysisInput.with_initial_data(
                **initial_data,
                description=_(f"""\
                        **External Assembly Analysis Ingestion**
                        Ingesting out-of-production generated Assembly Analysis results for study {study_accession} \
                        produced by [Assembly Analysis Pipeline V6](https://github.com/EBI-Metagenomics/assembly-analysis-pipeline).

                        {"**Biome tagger** Please select a Biome for the entire study " + f"[{ena_study.accession}: {ena_study.title}](https://www.ebi.ac.uk/ena/browser/view/{ena_study.accession})." if needs_biome else f"Biome is already set to {mgnify_study.biome} — change here if needed."}

                        {"**Webin owner** The study is private. Please provide the Webin account ID of the study owner so they can view their study." if needs_webin else ""}
                        """),
            ),
            timeout=EMG_CONFIG.slurm.default_flow_suspend_awaiting_input_timeout_secs,
        )

        biome = analyses.models.Biome.objects.get(path=ingest_analysis_input.biome.name)
        mgnify_study.biome = biome
        mgnify_study.save()
        logger.info(f"MGnify study {mgnify_study.accession} has biome {biome.path}.")

        if ingest_analysis_input.watchers:
            add_study_watchers(mgnify_study, ingest_analysis_input.watchers)

        validate_and_set_webin_owner(ena_study, ingest_analysis_input.webin_owner)
        mgnify_study.refresh_from_db()
    else:
        logger.info(
            f"Biome {mgnify_study.biome} was already set for this study. If a change is needed, do so in the DB Admin Panel."
        )
        logger.info(
            f"MGnify study has watchers: {mgnify_study.watchers.values_list('username', flat=True)}. Add more in the DB Admin Panel if needed."
        )

    if mgnify_study.is_private and not mgnify_study.webin_submitter:
        raise ValueError(
            f"Study {mgnify_study.accession} is private, but no webin owner was provided."
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

    logger.info("Setting ASA analysis states based on the end-of-execution reports...")
    set_asa_analysis_states(
        results_path / "asa",  # may as well be mgnify_study.results_dir / "asa"
        exported_analyses,
    )

    logger.info("Processing ASA results...")
    import_out_of_production_analysis_results(
        exported_analyses,
        results_path,
        AssemblyAnalysisPipeline.ASA,
    )

    logger.info("Processing VIRify results...")
    import_out_of_production_analysis_results(
        exported_analyses,
        results_path,
        AssemblyAnalysisPipeline.VIRIFY,
    )

    logger.info("Processing MAP results...")
    import_out_of_production_analysis_results(
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
    copy_out_of_production_assembly_analysis_results(
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
    Parse the samplesheet CSV to extract assembly accessions and validate required columns.

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

    try:
        with open(samplesheet_path, "r") as f:
            reader = csv.DictReader(f)

            if reader.fieldnames is None:
                raise ValueError("Samplesheet appears to be empty")

            actual_columns = set(reader.fieldnames)

            required_columns = {"sample", "assembly_fasta"}

            if not required_columns.issubset(actual_columns):
                raise ValueError(
                    f"Samplesheet is missing required columns: {required_columns - actual_columns}"
                )

            for row in reader:
                assembly_acc = row["sample"]
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
    flow_run_name="Import out-of-production generated analysis results: {pipeline_type}",
    retries=2,
    retry_delay_seconds=60,
)
def import_out_of_production_analysis_results(
    analyses: list[Analysis],
    results_path: Path,
    pipeline_type: AssemblyAnalysisPipeline,
) -> None:
    """
    Import out-of-production assembly analysis results.
    This flow validates the results of ASA/VIRify/MAP pipelines using the appropriate schema,
    marks analysis that failed validation as FAILED, and imports the results of analyses that
    passed validation into the database.

    :param analyses: List of Analysis objects to process
    :param results_path: Path to the directory containing out-of-production results
                        (should contain folders: asa/, virify/, map/)
    :param pipeline_type: The pipeline type (ASA, VIRify, or MAP)
    """
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
    validation_results = assembly_analysis_results_importer(
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
        import_results = assembly_analysis_results_importer(
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
def set_asa_analysis_states(study_results_dir: Path, analyses: List[Analysis]):
    """
    This function processes the end-of-execution reports generated by the assembly analysis pipeline
    to determine the outcome of each assembly and updates their statuses in the database accordingly.

    :param study_results_dir: Path to the directory containing the pipeline output
    :param analyses: List of assembly analyses objects to process
    :raises ValueError: If the analysed_assemblies.csv file is missing
    """
    logger = get_run_logger()

    qc_failed_csv = study_results_dir / "qc_failed_assemblies.csv"
    analysed_assemblies_csv = study_results_dir / "analysed_assemblies.csv"
    qc_failed_assemblies, analysed_assemblies = parse_asa_end_of_run_reports(
        qc_failed_csv, analysed_assemblies_csv, logger=logger
    )

    analyses_to_update = update_analyses_from_asa_reports(
        analyses,
        qc_failed_assemblies,
        analysed_assemblies,
        analysed_assemblies_csv,
        logger=logger,
    )

    # Bulk update - slightly gentler on the DB
    Analysis.objects.bulk_update(analyses_to_update, ["status"])


@task
def copy_out_of_production_assembly_analysis_results(
    study: Study, results_dir: Path, analyses: List[Analysis], timeout: int = 14400
):
    """
    Copy out-of-production assembly results to external storage and NFS mirror.

    FIXME: this function is very similar to copy_assembly_batch_results,
    needs refactoring (ochkalova).

    :param study: The study object to copy results for
    :param results_dir: The base path where the results are located
    :param analyses: The list of analyses objects to copy results for
    :param timeout: Timeout in seconds for each move operation (default: 4 hours)
    """

    study_prefix = accession_prefix_separated_dir_path(study.first_accession, -3)
    nfs_results_root = Path(results_dir) / "results"

    external_results_root = (
        settings.EMG_CONFIG.slurm.private_results_dir
        if study.is_private
        else settings.EMG_CONFIG.slurm.ftp_results_dir
    )

    external_copy_results = list(
        copy_out_of_production_analysis_results_to_destination_folder(
            analyses=analyses,
            results_workspace=Path(results_dir),
            destination_root=external_results_root,
            timeout=timeout,
        )
    )
    update_external_results_dirs_from_copy_results(
        external_copy_results,
        study_prefix=study_prefix,
    )
    update_analysis_statuses_from_copy_results(external_copy_results)

    # Copy the files to the NFS production
    nfs_copy_results = list(
        copy_out_of_production_analysis_results_to_destination_folder(
            analyses=analyses,
            results_workspace=Path(results_dir),
            destination_root=nfs_results_root,
            timeout=timeout,
        )
    )
    update_results_dirs_from_copy_results(
        nfs_copy_results,
        study_results_dir=nfs_results_root / study_prefix,
    )

    study.external_results_dir = str(study_prefix)
    study.save(update_fields=["external_results_dir"])


@task(
    description="Copy Out-of-Production Assembly Results",
    task_run_name="Copy out-of-production assembly results to {destination_root}",
)
def copy_out_of_production_analysis_results_to_destination_folder(
    analyses: list[Analysis],
    results_workspace: Path,
    destination_root: str | Path,
    timeout: int = 14400,
) -> list[BatchCopyResult]:
    """
    The source workspace is expected to have three pipeline subdirectories:
    ``asa/``, ``virify/``, and ``map/``, each with per-assembly result folders.

    :param analyses: List of analyses to copy results for
    :param results_workspace: Source workspace with ``asa/``, ``virify/`, and ``map/`` subdirectories
    :param destination_root: The root directory to copy results into
    :param timeout: Timeout in seconds for each move operation (default: 4 hours)
    :return: One copy result per ASA-completed analysis in the batch
    """
    logger = get_run_logger()

    destination_root = Path(destination_root)

    copy_results: list[BatchCopyResult] = []
    for analysis in analyses:
        copy_results.append(
            copy_single_analysis_results(
                analysis=analysis,
                results_workspace=results_workspace,
                destination_root=destination_root,
                timeout=timeout,
            )
        )

    logger.info(
        f"Completed copying out-of-production results for {len(copy_results)} analyses to {destination_root}"
    )

    return copy_results
