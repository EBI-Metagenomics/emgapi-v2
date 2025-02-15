from datetime import timedelta
from pathlib import Path
from textwrap import dedent
from typing import List

import django
import httpx
from django.conf import settings
from prefect import flow, get_run_logger, suspend_flow_run, task
from prefect.input import RunInput

from workflows.prefect_utils.slurm_policies import (
    ResubmitWithCleanedNextflowIfFailedPolicy,
)

django.setup()

from ena.models import Sample, Study
from workflows.prefect_utils.slurm_flow import (
    ClusterJobFailedException,
    run_cluster_job,
)


@task(
    name="Sample fetcher",
    task_run_name="Get samples for {study_accession}",
    retries=2,
    persist_result=True,
)
def fetch_samples(study_accession: str, limit: int) -> List[str]:
    """
    Get a list of samples for an ENA study.

    Note that this Task uses a "cache_key_fn", which makes a cache key using the input parameters.
    This is automatically saved to Prefect's "storage" (files on disk), so that if the same task inputs are
      tried again, the previous result will be used instead of executing again. This is also why re return the
      accession strings instead of Sample objects - they are easier to serialize to a file.

    :param study_accession: The study accession
    :param limit: The max number of samples to fetch
    :return: List of sample accession
    """
    logger = get_run_logger()
    logger.info(f"Will fetch study {study_accession} samples from ENA portal API")
    study = Study.objects.get(accession=study_accession)
    portal = httpx.get(
        f'https://www.ebi.ac.uk/ena/portal/api/search?result=sample&query="study_accession={study_accession}"&limit={limit}&format=json'
    )
    accessions_created = []
    if portal.status_code == httpx.codes.OK:
        for sample in portal.json():
            if isinstance(sample, dict):
                Sample.objects.get_or_create(
                    accession=sample["sample_accession"], study=study
                )
                accessions_created.append(sample["sample_accession"])
            else:
                logger.warning(f"Portal API response looks strange... {sample}")
    else:
        logger.warning(
            f"Bad response from portal api... {portal.status_code} {portal.text}"
        )
    return accessions_created


class DownloadOptionsInput(RunInput):
    samples_limit: int


@flow(
    name="Download a study read-runs",
    flow_run_name="Download read-runs for study: {accession}",
)
def realistic_example(accession: str):
    """
    Example flow for Prefect, doing some "realistic" work.
    Downloads read-runs from ENA using a minimal nextflow pipeline, and integrated with the Django DB.

    :param accession: Accession of ENA Study to download
    :return:
    """
    logger = get_run_logger()
    logger.info("Hello from the realistic example")

    # Make a study.
    study, created = Study.objects.get_or_create(
        accession=accession, defaults={"title": "unknown"}
    )
    if created:
        logger.info(f"I created an ENA study object: {study}")

    # Example of how to pause the flow to wait for input from the team.
    # This will stop the flow. It can be resumed by going to the Prefect admin panel, and filling in the
    # required info into the popup.
    download_options: DownloadOptionsInput = suspend_flow_run(
        wait_for_input=DownloadOptionsInput.with_initial_data(
            samples_limit=10,
            description=dedent(
                f"""\
                **ENA Downloader**
                This will download read-runs from ENA.

                Please pick how many samples (the max limit) to download for the study {study.accession}.
            """
            ),
        )
    )

    logger.info(
        f"Will download up to {download_options.samples_limit} samples for {study.accession}"
    )

    # Get samples from ENA portal API.
    # Even if this flow needs to be restarted, the actual fetch should only happen once thanks to prefect caching.
    sample_accessions = fetch_samples(study.accession, download_options.samples_limit)

    # Now use our helpers to execute a nextflow pipeline on Slurm.
    # This run_cluster_job helper orchestrates the work on slurm, makes some Django and Prefect Artefacts to document
    #   the job being run, and waits until the job is done.
    # Should this top level "realistic example" flow crash and need to be re-run, this helper SHOULD connect to the
    #   previously started job - assuming all the options remain the same. This means cluster resources are not wasted.

    slurm_job_results = []
    for sample_accession in sample_accessions[:5]:
        # limit to five sequential jobs.
        # to achieve parallelisation, our approach is to use "samplesheets" in nextflow.
        try:
            orchestrated_cluster_job = run_cluster_job(
                name=f"Download read-runs for for study {study.accession} sample {sample_accession}",
                command=(
                    f"nextflow run {settings.EMG_CONFIG.slurm.pipelines_root_dir}/download_read_runs.nf "
                    f"-resume "
                    f"-name fetch-read-runs-{study.accession}-{sample_accession} "
                    f"--sample {sample_accession} "
                    f"-ansi-log false "  # otherwise the logs in prefect/django are full of control characters
                    f"-with-trace trace-{sample_accession}.txt"
                ),
                expected_time=timedelta(hours=1),
                memory="500M",
                resubmit_policy=ResubmitWithCleanedNextflowIfFailedPolicy,
                # These policies control what happens when identical jobs are submitted in future,
                #   including when a flow crashes and is restarted.
                # This policy says that if an identical job is started in future, it won't actually start anything
                #   in slurm, unless the last identical job resulted in a FAILED slurm job, in which case we will
                #   start a new one to try again.
                # The "cleaned nextflow" part refers the policy including a `nextflow clean` command that should be
                #   run before the main command is resubmitted.
                working_dir=Path(settings.EMG_CONFIG.slurm.default_workdir)
                / "realistic-example"
                / "realistic-example-workdir",
                environment="ALL",  # copy env vars from the prefect agent into the slurm job
            )
        except ClusterJobFailedException as e:
            # We can optionally handle errors by catching this exception, rather than crashing the entire flow.
            logger.warning(
                f"Something went wrong running pipeline for {sample_accession}"
            )
            logger.error(e)
            slurm_job_results.append(False)
        else:
            slurm_job_results.append(orchestrated_cluster_job)

    for sample, job_result in zip(sample_accessions, slurm_job_results):
        if job_result:
            logger.info(f"Successfully ran nextflow pipeline for {sample}")
        else:
            logger.error(f"Something went wrong running nextflow pipeline for {sample}")
