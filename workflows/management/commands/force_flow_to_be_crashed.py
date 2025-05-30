import logging

from django.core.management.base import BaseCommand, CommandError
from prefect import get_client, State
from prefect.exceptions import ObjectNotFound
from prefect.server.schemas.states import StateType


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Command(BaseCommand):
    help = (
        "Force mark a prefect flow as crashed. Does not impact any parent or subflows."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "-f",
            "--flow_run_id",
            type=str,
            help="The UUID of the flow run to be forcibly transitioned to Crashed state",
            required=True,
        )
        parser.add_argument(
            "-m",
            "--message",
            type=str,
            help="Optional detail message to attach to the Crashed state, e.g. explaining why.",
            required=False,
        )

    def handle(self, *args, **options):
        with get_client(sync_client=True) as client:
            try:
                flow_run = client.read_flow_run(options["flow_run_id"])
            except ObjectNotFound:
                raise CommandError(f"No flow run found for {options['flow_run_id']}")

            crashed_state = State(
                type=StateType.CRASHED,
                name="Crashed (Manually)",
                message=options["message"] or None,
            )

            logger.info(f"Run is {flow_run}")

            client.set_flow_run_state(flow_run.id, crashed_state, force=True)

            flow_run_now = client.read_flow_run(flow_run.id)
            logger.info(f"Run is now {flow_run_now}")
