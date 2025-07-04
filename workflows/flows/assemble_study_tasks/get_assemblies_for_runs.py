from typing import List

from prefect import task, get_run_logger
from prefect.cache_policies import DEFAULT

import analyses.models
from workflows.ena_utils.ena_api_requests import ENALibraryStrategyPolicy


@task(
    retries=2,
    task_run_name="Create/get assembly objects for read_runs in study: {study_accession}",
    cache_policy=DEFAULT,
)
def get_or_create_assemblies_for_runs(
    study_accession: str,
    read_runs: List[str],
    library_strategy_policy: ENALibraryStrategyPolicy = ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA,
) -> List[str]:
    logger = get_run_logger()
    study = analyses.models.Study.objects.get(accession=study_accession)
    logger.info(f"Getting/creating assemblies for study {study_accession}")
    assembly_ids = []
    for read_run in read_runs:
        logger.info(f"Getting/creating assembly for run {read_run}")
        run = analyses.models.Run.objects.get(ena_accessions__icontains=read_run)
        if run.experiment_type not in [
            run.ExperimentTypes.METAGENOMIC,
            run.ExperimentTypes.METATRANSCRIPTOMIC,
        ]:
            logger.warning(
                f"Run {run.first_accession} is a {run.experiment_type} experiment type, not metagenomic/metatranscriptomic."
            )
            if (
                library_strategy_policy
                == ENALibraryStrategyPolicy.ASSUME_OTHER_ALSO_MATCHES
                and run.experiment_type == run.ExperimentTypes.UNKNOWN
            ):
                logger.warning(
                    f"But, creating assembly anyway since {run.first_accession} is of unknown experiment type."
                )
            elif library_strategy_policy == ENALibraryStrategyPolicy.OVERRIDE_ALL:
                logger.warning(
                    "But, creating assembly anyway since library strategy is overridden by policy."
                )
            else:
                logger.warning(
                    f"So, not creating assembly for run {run.first_accession}."
                )
                continue

        assembly, created = analyses.models.Assembly.objects.get_or_create(
            run=run,
            ena_study=study.ena_study,
            reads_study=study,
            defaults={"is_private": run.is_private, "sample": run.sample},
        )
        if created:
            logger.info(f"Created assembly {assembly}")
        else:
            logger.info(f"Found assembly {assembly}")
        assembly_ids.append(assembly.id)
    return assembly_ids
