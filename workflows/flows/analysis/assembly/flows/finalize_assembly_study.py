from prefect import flow, get_run_logger

import analyses.models
import workflows.models
from workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results import (
    copy_v6_study_summaries,
)
from workflows.flows.analyse_study_tasks.shared.study_summary import (
    merge_assembly_study_summaries,
)
from workflows.flows.analysis.assembly.tasks.add_assembly_study_summaries_to_downloads import (
    add_assembly_study_summaries_to_downloads,
)
from workflows.models import AssemblyAnalysisPipelineStatus


@flow(
    name="Generate the study summary and mark the study as completed.",
    flow_run_name="Finalize assembly: {study_accession}",
)
def finalize_assembly_study(study_accession: str):
    """
    Run the last study level steps on study assembly analysis batches

    This includes:
    - Merging assembly study summaries
    - Adding summaries to downloads
    - Copying v6 study summaries
    - Updating study features

    :param study_accession: Study accession (e.g., MGYS00001234)
    :type study_accession: str
    """
    logger = get_run_logger()

    mgnify_study = analyses.models.Study.objects.get(accession=study_accession)

    # Check if all batches are complete
    batches = workflows.models.AssemblyAnalysisBatch.objects.filter(
        study=mgnify_study,
        pipeline_versions__in=[analyses.models.Analysis.PipelineVersions.v6],
    )

    logger.info(f"Found {batches.count()} batches for study {study_accession}")

    # Check for incomplete batches
    incomplete_count = 0
    for batch in batches:
        incomplete_analyses = batch.batch_analyses.exclude(
            asa_status=AssemblyAnalysisPipelineStatus.COMPLETED
        )
        if incomplete_analyses.exists():
            incomplete_count += incomplete_analyses.count()

    if incomplete_count > 0:
        logger.warning(
            f"{incomplete_count} analyses still incomplete. Skipping finalization."
        )
        return

    logger.info("All batches complete. Running finalization steps...")

    ################################################
    #  === Study summary merging and uploading === #
    ################################################
    merge_assembly_study_summaries(
        mgnify_study.accession,
        cleanup_partials=True,
    )

    #####################################
    #  === Download files summaries === #
    #####################################
    add_assembly_study_summaries_to_downloads(mgnify_study.accession)

    copy_v6_study_summaries(mgnify_study.accession)

    mgnify_study.refresh_from_db()

    # Sanity check
    mgnify_study.features.has_v6_analyses = mgnify_study.analyses.filter(
        pipeline_version=analyses.models.Analysis.PipelineVersions.v6, is_ready=True
    ).exists()

    mgnify_study.save()

    logger.info(f"Finalization complete for study {study_accession}")
