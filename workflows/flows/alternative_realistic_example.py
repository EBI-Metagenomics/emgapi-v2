import subprocess
from pathlib import Path
from textwrap import dedent as _
from typing import List

import httpx
from prefect import flow, get_run_logger, suspend_flow_run, task
from prefect.input import RunInput
from prefect.task_runners import ThreadPoolTaskRunner
from prefect.artifacts import create_markdown_artifact
from activate_django_first import EMG_CONFIG
from ena.models import Sample, Study


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


@task(
    name="Download read-runs",
    task_run_name="Download read-runs for for study {study_accession} sample {sample_accession}",
    retries=2,
    persist_result=True,
)
def download_read_runs(study_accession: str, sample_accession: str) -> List[str]:
    """
    Download read-runs for a given sample accession.

    :param study_accession: The study accession
    :param sample_accession: The sample accession
    :return: List of paths to retrieved read-runs
    """
    logger = get_run_logger()
    logger.info(f"Will download read-runs for sample {sample_accession} from ENA")

    workdir = (
        Path(EMG_CONFIG.slurm.default_workdir)
        / "alternative_realistic_example"
        / study_accession
        / sample_accession
    )

    publish_dir = workdir / "results"

    workdir.mkdir(exist_ok=True, parents=True)

    with subprocess.Popen(
        (
            "echo $(pwd) && "
            f"nextflow run {EMG_CONFIG.slurm.pipelines_root_dir}/download_read_runs.nf "
            f"-resume "
            f"--sample {sample_accession} "
            f"--publishDir {publish_dir} "
            f"-ansi-log false "  # otherwise the logs in prefect/django are full of control characters
            f"-with-trace trace-{sample_accession}.txt"
        ),
        cwd=workdir,
        shell=True,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
    ) as process:
        for line in process.stdout:
            logger.info(line)

    return publish_dir


class DownloadOptionsInput(RunInput):
    samples_limit: int


@flow(
    name="Download a study read-runs",
    flow_run_name="Download read-runs for study: {accession}",
)
def alternative_realistic_example(accession: str):
    """
    Example flow for Prefect, doing some "realistic" work.
    Downloads read-runs from ENA using a minimal nextflow pipeline, and integrated with the Django DB.

    This will run at most 5 tasks (read-runs downloads) in parallel
    In the end it produces a markdown artifact with paths to read-run files

    :param accession: Accession of ENA Study to download
    :return:
    """
    logger = get_run_logger()
    logger.info("Hello from the realistic example")

    study, created = Study.objects.get_or_create(
        accession=accession, defaults={"title": "unknown"}
    )

    if created:
        logger.info(f"I created an ENA study object: {study}")

    download_options: DownloadOptionsInput = suspend_flow_run(
        wait_for_input=DownloadOptionsInput.with_initial_data(
            samples_limit=10,
            description=_(
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

    sample_accessions = fetch_samples(study.accession, download_options.samples_limit)

    with ThreadPoolTaskRunner(max_workers=5) as runner:
        futures = runner.map(
            download_read_runs,
            parameters={
                "study_accession": study.accession,
                "sample_accession": sample_accessions,
            },
        )

        result_dirs = futures.result()

    samples = "\n".join(
        [
            f"- {sample_accession}: `{result_dir}`"
            for sample_accession, result_dir in zip(sample_accessions, result_dirs)
        ]
    )
    markdown = f"# Study {study.accession}\n" f"## Samples\n" f"{samples}"

    create_markdown_artifact(key="pipeline-report", markdown=markdown)
