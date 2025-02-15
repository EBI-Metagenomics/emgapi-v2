from typing import List

from prefect import task

import analyses.models


@task(
    retries=2,
    task_run_name="Create/get assembly objects for read_runs in study: {study.accession}",
    log_prints=True,
)
def get_or_create_assemblies_for_runs(
    study: analyses.models.Study, read_runs: List[str]
) -> List[str]:
    assembly_ids = []
    for read_run in read_runs:
        run = analyses.models.Run.all_objects.get(ena_accessions__icontains=read_run)
        if run.experiment_type not in [
            run.ExperimentTypes.METAGENOMIC,
            run.ExperimentTypes.METATRANSCRIPTOMIC,
        ]:
            print(
                f"Not creating assembly for run {run.first_accession} because it is a {run.experiment_type}"
            )
            continue

        assembly, created = analyses.models.Assembly.all_objects.get_or_create(
            run=run,
            ena_study=study.ena_study,
            reads_study=study,
            defaults={"is_private": run.is_private},
        )
        if created:
            print(f"Created assembly {assembly}")
        assembly_ids.append(assembly.id)
    return assembly_ids
