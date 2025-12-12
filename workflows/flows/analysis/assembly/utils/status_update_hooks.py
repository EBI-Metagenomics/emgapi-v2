import logging

from django.db import close_old_connections

from workflows.models import AssemblyAnalysisBatch

logger = logging.getLogger(__name__)


def update_batch_status_counts(flow, flow_run, state) -> None:
    """
    Update all pipeline status counts after flow completion or failure.

    This hook should be attached to a Prefect flow's on_completion and on_failure
    hooks to ensure status counts are always in sync with actual analysis states.

    The batch_id is extracted from the flow run's assembly_analyses_batch_id parameter.

    :param flow: The Prefect Flow object
    :param flow_run: The Prefect flow run object
    :param state: The Prefect State object
    """
    try:
        close_old_connections()

        # Extract batch_id from flow run parameters
        batch_id = flow_run.parameters.get("assembly_analyses_batch_id")

        if not batch_id:
            logger.error(
                "Could not find assembly_analyses_batch_id in flow run parameters"
            )
            return

        batch = AssemblyAnalysisBatch.objects.get(id=batch_id)
        batch.update_pipeline_status_counts()
        logger.info(f"Updated status counts for batch {batch_id}")

    except AssemblyAnalysisBatch.DoesNotExist:
        logger.error(f"Batch not found, flow run parameters: {flow_run.parameters}")
    except Exception as e:
        logger.error(f"Failed to update status counts: {e}", exc_info=True)
