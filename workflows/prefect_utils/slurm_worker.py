from typing import Optional, Dict, Any
import asyncio

import anyio.abc
from prefect import get_client
from prefect.states import Running
from pydantic import Field
from prefect.workers.base import (
    BaseWorker,
    BaseWorkerResult,
    BaseJobConfiguration,
    BaseVariables,
)
import httpx

from activate_django_first import EMG_CONFIG
from workflows.prefect_utils.slurm_status import (
    SlurmStatus,
    slurm_status_is_finished_successfully,
    slurm_status_is_finished_unsuccessfully,
)


class SlurmJobConfiguration(BaseJobConfiguration):
    partition: str = Field(
        default="main", description="Slurm partition to submit job to."
    )
    time: str = Field(default="01:00:00", description="Max runtime for the job.")
    nfs_path: str = Field(..., description="NFS path to code to run.")
    script: str = Field(default="run.sh", description="Script to execute in the job.")
    poll_interval: int = Field(
        default=EMG_CONFIG.slurm.default_seconds_between_job_checks,
        description="Interval in seconds between job status checks.",
    )
    max_polls: int = Field(
        default=EMG_CONFIG.slurm.default_job_status_checks_limit,
        description="Maximum number of status checks before giving up.",
    )


class SlurmJobVariables(BaseVariables):
    partition: str = Field(
        default="main", description="Slurm partition to submit job to."
    )
    time: str = Field(default="01:00:00", description="Max runtime for the job.")
    nfs_path: str = Field(..., description="NFS path to code to run.")
    script: str = Field(default="run.sh", description="Script to execute in the job.")
    poll_interval: int = Field(
        default=EMG_CONFIG.slurm.default_seconds_between_job_checks,
        description="Interval in seconds between job status checks.",
    )
    max_polls: int = Field(
        default=EMG_CONFIG.slurm.default_job_status_checks_limit,
        description="Maximum number of status checks before giving up.",
    )


class SlurmWorkerResult(BaseWorkerResult):
    job_id: Optional[int] = None
    job_state: Optional[str] = None
    job_details: Optional[Dict[str, Any]] = None


class SlurmWorker(BaseWorker):
    type: str = "slurm"
    job_configuration = SlurmJobConfiguration
    job_configuration_variables = SlurmJobVariables
    _documentation_url = "https://slurm.schedmd.com/rest.html"
    _description = "Prefect worker that submits jobs to Slurm via REST API."

    async def get_job_status(self, job_id: int) -> Dict[str, Any]:
        """
        Poll the Slurm REST API for job status.

        Args:
            job_id: The Slurm job ID to check

        Returns:
            A dictionary containing job status information
        """
        # Construct the job status URL
        # The Slurm REST API endpoint for job status is typically /slurm/v0.0.36/job/{job_id}
        # job_status_url = f"{EMG_CONFIG.slurm.slurm_poll_url}/{job_id}"
        job_status_url = f"{EMG_CONFIG.slurm.slurm_poll_url}"

        headers = {
            "X-SLURM-USER-NAME": EMG_CONFIG.slurm.slurm_user_name,
            "X-SLURM-USER-TOKEN": EMG_CONFIG.slurm.slurm_user_token,
        }

        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(job_status_url, headers=headers)
                response.raise_for_status()
                job_info = response.json()
                return job_info
        except httpx.HTTPError as e:
            # If we can't get the job status, return a minimal dict with unknown state
            return {"job_state": SlurmStatus.unknown, "error": str(e)}

    async def run(
        self,
        flow_run,
        configuration: SlurmJobConfiguration,
        task_status: Optional[anyio.abc.TaskStatus] = None,
    ) -> SlurmWorkerResult:
        # Compose the Slurm job script
        job_script = f"""#!/bin/bash
set -e
cd {configuration.nfs_path}
chmod +x {configuration.script}
./{configuration.script}
"""
        logger = self.get_flow_run_logger(flow_run)
        # Submit job to Slurm REST API
        slurm_submit_url = EMG_CONFIG.slurm.slurm_submit_url
        headers = {
            "X-SLURM-USER-NAME": EMG_CONFIG.slurm.slurm_user_name,
            "X-SLURM-USER-TOKEN": EMG_CONFIG.slurm.slurm_user_token,
        }
        data = {
            "script": job_script,
            "job": {
                "partition": configuration.partition,
                "time_limit": configuration.time,
                "name": configuration.name or f"prefect-{flow_run.id}",
            },
        }

        logger.info(f"Submitting job to Slurm REST API: {slurm_submit_url}")

        async with httpx.AsyncClient() as client:
            response = await client.post(slurm_submit_url, json=data, headers=headers)
            response.raise_for_status()
            job_info = response.json()
            job_id = job_info.get("job_id")
            logger.info(f"Job submitted to Slurm REST API: {job_id}")

        if task_status:
            task_status.started(job_id)

        # Poll for job completion
        poll_count = 0
        job_state = SlurmStatus.pending.value
        job_details = {}

        while poll_count < configuration.max_polls:
            logger.info(f"Polling Slurm job {job_id} status")
            # Wait for the configured interval before polling again
            await asyncio.sleep(configuration.poll_interval)

            # Get the current job status
            job_details = await self.get_job_status(job_id)
            job_state = job_details.get("job_state", SlurmStatus.unknown.value)

            # Log the current status
            logger.info(f"Job {job_id} status: {job_state}")
            async with get_client() as client:
                await client.set_flow_run_state(
                    flow_run_id=flow_run.id,
                    state=Running(
                        message=f"Polling: Slurm job {job_id} state = {job_state}"
                    ),
                )

            # Check if the job has completed or failed
            if slurm_status_is_finished_successfully(job_state):
                logger.info(f"Job {job_id} completed successfully")
                break

            if slurm_status_is_finished_unsuccessfully(job_state):
                error_message = job_details.get(
                    "error", f"Job failed with state {job_state}"
                )
                logger.error(f"Job {job_id} failed: {error_message}")
                return SlurmWorkerResult(
                    status_code=1,
                    identifier=str(job_id),
                    job_id=job_id,
                    job_state=job_state,
                    job_details=job_details,
                )

            poll_count += 1

        # Check if we reached the maximum number of polls
        if (
            poll_count >= configuration.max_polls
            and not slurm_status_is_finished_successfully(job_state)
        ):
            logger.warning(
                f"Reached maximum number of polls ({configuration.max_polls}) for job {job_id}"
            )
            return SlurmWorkerResult(
                status_code=2,
                identifier=str(job_id),
                job_id=job_id,
                job_state=job_state,
                job_details=job_details,
            )

        return SlurmWorkerResult(
            status_code=0,
            identifier=str(job_id),
            job_id=job_id,
            job_state=job_state,
            job_details=job_details,
        )
