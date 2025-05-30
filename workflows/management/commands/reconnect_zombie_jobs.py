import logging
from datetime import timedelta
from uuid import UUID

from django.core.management.base import BaseCommand
from django.utils.timezone import now
from pendulum import DateTime
from prefect import get_client, State
from prefect.client.orchestration import SyncPrefectClient
from prefect.server.schemas.states import StateType

from workflows.models import OrchestratedClusterJob
from workflows.prefect_utils.find_parent_flow_run import (
    find_parent_flow_runs_recursively,
)
from workflows.prefect_utils.slurm_status import SlurmStatus


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Command(BaseCommand):
    help = "Finds zombie jobs – cluster jobs whose prefect orchestrating flows seems to have died – and restarts their flows."

    def add_arguments(self, parser):
        parser.add_argument(
            "-t",
            "--tolerance_seconds",
            type=int,
            help="How many seconds may have elapsed since a cluster job was last updated, to be not considered zombie.",
            required=True,
        )

    @staticmethod
    def crash_flow(
        client: SyncPrefectClient,
        flow_run_id: UUID,
        state_name: str,
        state_message: str | None,
    ) -> State:
        crashed_state = State(
            type=StateType.CRASHED, name=state_name, message=state_message
        )
        client.set_flow_run_state(flow_run_id, crashed_state, force=True)

    def handle_zombie_orchestrated_cluster_job(
        self, orchestrated_cluster_job: OrchestratedClusterJob, tolerance: DateTime
    ):
        with get_client(sync_client=True) as client:
            logger.info(f"Handling {orchestrated_cluster_job}")
            flow_run_id = orchestrated_cluster_job.flow_run_id
            logger.info(f"Flow run ID {orchestrated_cluster_job.flow_run_id}")

            flow_run = client.read_flow_run(flow_run_id)
            logger.info(f"Flow run is {flow_run.id}")
            logger.debug(f"Flow run detail {flow_run}")
            if not flow_run.state.is_running():
                logger.info(f"But flow run is {flow_run.state}")
                return

            logger.info("Flow is supposedly running")

            if flow_run.updated > tolerance:
                logger.info(f"But flow run was updated recently at {flow_run.updated}")
                return

            logger.info(
                "Flow run was also updated longer ago than zombie-tolerance time"
            )

            logger.warning("Crashing flow")

            self.crash_flow(
                client=client,
                flow_run_id=flow_run_id,
                state_name="Crashed (Zombie)",
                state_message="Crashed by reconnect_zombie_job",
            )

            flow_run_post = client.read_flow_run(flow_run_id)
            logger.info(f"Flow run now is {flow_run_post.state}")

            parent_flow_runs = find_parent_flow_runs_recursively(flow_run_post)

            for parent_flow_run in parent_flow_runs:
                logger.info(
                    f"Parent flow run {parent_flow_run.id} will also be crashed"
                )
                self.crash_flow(
                    client=client,
                    flow_run_id=parent_flow_run.id,
                    state_name="Crashed (Zombie)",
                    state_message="Crashed by reconnect_zombie_job, as parent of a zombie flow",
                )

            head_flow_run = parent_flow_runs[-1] if parent_flow_runs else flow_run_post
            # the one to restart
            restart_state = State(
                type=StateType.SCHEDULED,
                name="Awaiting restart",
                message="Restarted by reconnect_zombie_job, since this or a subflow was in a zombie state",
            )
            client.set_flow_run_state(flow_run_id=head_flow_run.id, state=restart_state)

            head_flow_run_post = client.read_flow_run(head_flow_run.id)
            logger.info(
                f"Restarted head flow run {head_flow_run.id} / {head_flow_run.name}: state is now {head_flow_run_post.state}"
            )

    def handle(self, *args, **options):
        zombies_if_before = now() - timedelta(seconds=options["tolerance_seconds"])
        logger.info(
            f"Looking for RUNNING cluster jobs last updated before {zombies_if_before.isoformat()}"
        )

        probable_zombie_jobs = OrchestratedClusterJob.objects.filter(
            last_known_state=SlurmStatus.running, state_checked_at__lt=zombies_if_before
        )
        logger.info(f"Found {probable_zombie_jobs.count()} such jobs")

        for ocj in probable_zombie_jobs:
            self.handle_zombie_orchestrated_cluster_job(
                ocj, tolerance=zombies_if_before
            )
