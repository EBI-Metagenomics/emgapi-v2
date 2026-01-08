from prefect import flow, get_run_logger
from prefect.flow_engine import load_flow_run

from activate_django_first import EMG_CONFIG  # noqa

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


@flow(
    name="Generate the study summary and mark the study as completed.",
    flow_run_name="Finalize assembly: {study_accession}",
)
def finalize_assembly_study(study_accession: str):
    """
    Run the last study level steps on study assembly analysis batches.
    This method checks that all the study batches have finished running, as in each
    batch 3 flows (aca, virify and map) are not running anymore. This is not ideal, and I'm
    working to improve it.
    TODO: Improve as is_running() is not enough, flows may have failed or haven't even started!
    TODO: Add unit tests
    TODO: Use events or automations to improve this

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
        pipeline_versions__contains=analyses.models.Analysis.PipelineVersions.v6,
    )

    logger.info(f"Found {batches.count()} batches for study {study_accession}")

    # Check for incomplete batches
    incomplete_count = 0
    for batch in batches:
        # Each batch has 3 potential flows: aca, virify and map.
        # This is a blunt approach of fetching those flows and making sure they are not running
        flow_ids = [
            batch.asa_flow_run_id,
            batch.virify_flow_run_id,
            batch.map_flow_run_id,
        ]
        for flow_id in flow_ids:
            try:
                if load_flow_run(flow_id).is_running():
                    incomplete_count += 1
                    break
            except Exception:
                logger.error(f"Flow ID {flow_id} is not an integer. Skipping...")
                continue

    if incomplete_count > 0:
        logger.warning(
            f"{incomplete_count} flows are still running. Skipping finalization."
        )
        return

    logger.info(
        "All batches flows have finished running. Running finalization steps..."
    )

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

    mgnify_study.features.has_v6_analyses = mgnify_study.analyses.filter(
        pipeline_version=analyses.models.Analysis.PipelineVersions.v6, is_ready=True
    ).exists()

    mgnify_study.save()

    logger.info(f"Finalization complete for study {study_accession}")
