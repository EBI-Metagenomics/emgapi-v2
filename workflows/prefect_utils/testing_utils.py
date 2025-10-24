import csv
import gzip
import pathlib
import shutil
import time
from dataclasses import dataclass
from logging import LogRecord
from pathlib import Path
from typing import Any, Callable, List, Tuple

from prefect import State, get_client
from prefect.client.schemas.filters import LogFilter, LogFilterFlowRunId
from pydantic import UUID4


def get_logs_for_flow_run(flow_run_id: UUID4) -> str:
    with get_client(sync_client=True) as client:
        logs = client.read_logs(
            log_filter=LogFilter(flow_run_id=LogFilterFlowRunId(any_=[flow_run_id]))
        )
        return " ".join(log.message for log in logs)


@dataclass
class LoggedFlowRunResult:
    logs: str
    result: Any


def run_flow_and_capture_logs(flow: Callable, *args, **kwargs) -> LoggedFlowRunResult:
    """
    Run a prefect flow, and then pull the logs for it from the prefect server API.
    This is a tedious workaround for buggy behaviour in capturing prefect logs with pytest caplog.

    E.g.

    @flow
    def my_flow(widget="blue"):
        logger = get_run_logger()
        logger.info(f"The widget will be {widget}")
        return f"{widget.upper()} WIDGET"

    my_logged_flow = run_flow_and_capture_logs(my_flow, "red")
    assert "will be red" in my_logged_flow.logs
    assert my_logged_flow.result == "RED WIDGET"
    """
    state: State = flow(*args, return_state=True, **kwargs)
    time.sleep(0.5)  # wait for log flushing
    logs = get_logs_for_flow_run(state.state_details.flow_run_id)
    return LoggedFlowRunResult(logs=logs, result=state.result())


async def run_async_flow_and_capture_logs(flow: Callable, *args, **kwargs):
    """
    Run an async prefect flow, and then pull the logs for it from the prefect server API.
    This is a tedious workaround for buggy behaviour in capturing prefect logs with pytest caplog.

    E.g.

    @flow
    async def my_flow(widget="blue"):
        logger = get_run_logger()
        await something()
        logger.info(f"The widget will be {widget}")
        return f"{widget.upper()} WIDGET"

    my_logged_flow = await run_async_flow_and_capture_logs(my_flow, "red")
    assert "will be red" in my_logged_flow.logs
    assert my_logged_flow.result == "RED WIDGET"
    """
    state: State = await flow(*args, return_state=True, **kwargs)
    logs = get_logs_for_flow_run(state.state_details.flow_run_id)
    return LoggedFlowRunResult(
        logs=logs, result=state.result(), flow_run_id=state.state_details.flow_run_id
    )


def should_not_mock_httpx_requests_to_prefect_server(request):
    return request.url.host != "127.0.0.1"


def combine_caplog_records(caplog_records: list[LogRecord]):
    return "\n".join([rec.getMessage() for rec in caplog_records])


def write_empty_fasta_file(path: pathlib.Path):
    """
    Pyfastx (for example) requires a fasta file to start with a fasta header at least.
    This makes a minimal fasta file that will pass such validation.
    """
    opener = gzip.open if path.suffix.endswith(".gz") else open
    with opener(path, "w") as f:
        f.write(">empty\n")


def generate_assembly_v6_pipeline_results(
    asa_workspace: Path,
    assemblies: List[Tuple[str, str]],
    copy_from_fixtures: Path = None,
):
    """
    Generate fake ASA pipeline v6 output files for testing.
    This mimics what the actual assembly analysis pipeline would produce.

    :param asa_workspace: ASA workspace directory where results should be created
    :param assemblies: List of tuples (assembly_accession, status) where status is 'success' or 'qc_failed'
    :param copy_from_fixtures: Optional path to fixture directory to copy real results from
    """
    asa_workspace.mkdir(parents=True, exist_ok=True)

    # Track successful and failed assemblies
    successful_assemblies = []
    qc_failed_assemblies = []

    for assembly_accession, status in assemblies:
        if status == "success":
            successful_assemblies.append(assembly_accession)
        elif status == "qc_failed":
            qc_failed_assemblies.append(assembly_accession)
        else:
            raise ValueError(
                f"Invalid status '{status}' for assembly {assembly_accession}"
            )

        # If copy_from_fixtures provided, copy the real results
        if copy_from_fixtures:
            source_assembly_dir = copy_from_fixtures / assembly_accession
            dest_assembly_dir = asa_workspace / assembly_accession
            if source_assembly_dir.exists():
                shutil.copytree(
                    source_assembly_dir, dest_assembly_dir, dirs_exist_ok=True
                )
        else:
            # Create minimal directory structure
            assembly_dir = asa_workspace / assembly_accession
            assembly_dir.mkdir(exist_ok=True, parents=True)

            # Create minimal required files/directories
            (assembly_dir / "qc").mkdir(exist_ok=True)
            (assembly_dir / "taxonomy").mkdir(exist_ok=True)
            (assembly_dir / "cds").mkdir(exist_ok=True)

    # Create analysed_assemblies.csv for successful assemblies
    if successful_assemblies:
        with (asa_workspace / "analysed_assemblies.csv").open("w", newline="") as f:
            writer = csv.writer(f)
            for assembly_accession in successful_assemblies:
                writer.writerow([assembly_accession, "success"])

    # Create qc_failed_assemblies.csv for failed assemblies
    if qc_failed_assemblies:
        with (asa_workspace / "qc_failed_assemblies.csv").open("w", newline="") as f:
            writer = csv.writer(f)
            for assembly_accession in qc_failed_assemblies:
                writer.writerow([assembly_accession, "failed"])

    # Create downstream samplesheets directory for VIRify/MAP pipelines
    downstream_samplesheets_dir = asa_workspace / "downstream_samplesheets"
    downstream_samplesheets_dir.mkdir(exist_ok=True)

    # Copy VIRify samplesheet if it exists in fixtures
    if (
        copy_from_fixtures
        and (
            copy_from_fixtures / "downstream_samplesheets" / "virify_samplesheet.csv"
        ).exists()
    ):
        (downstream_samplesheets_dir / "virify_samplesheet.csv").write_text(
            (
                copy_from_fixtures
                / "downstream_samplesheets"
                / "virify_samplesheet.csv"
            ).read_text()
        )

    return asa_workspace
