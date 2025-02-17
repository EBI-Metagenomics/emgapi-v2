from workflows.flows.analyse_study_tasks.create_analyses import create_analyses
from workflows.flows.analyse_study_tasks.get_analyses_to_attempt import (
    get_analyses_to_attempt,
)
from workflows.flows.analyse_study_tasks.run_amplicon_pipeline_in_chunks import (
    run_amplicon_pipeline_in_chunks,
)

from prefect import flow, get_run_logger

from activate_django_first import EMG_CONFIG

import analyses.models
import ena.models
from workflows.ena_utils.ena_api_requests import (
    get_study_from_ena,
    get_study_readruns_from_ena,
)
from workflows.prefect_utils.analyses_models_helpers import (
    chunk_list,
)


@flow(
    name="Run analysis pipeline-v6 on amplicon study",
    log_prints=True,
    flow_run_name="Analyse amplicon: {study_accession}",
)
def analysis_amplicon_study(study_accession: str):
    """
    Get a study from ENA, and input it to MGnify.
    Kick off amplicon-v6 pipeline.
    :param study_accession: Study accession e.g. PRJxxxxxx
    """
    logger = get_run_logger()
    # Create/get ENA Study object
    ena_study = ena.models.Study.objects.get_ena_study(study_accession)
    if not ena_study:
        ena_study = get_study_from_ena(study_accession)
        ena_study.refresh_from_db()
    logger.info(f"ENA Study is {ena_study.accession}: {ena_study.title}")

    # Get a MGnify Study object for this ENA Study
    mgnify_study = analyses.models.Study.objects.get_or_create_for_ena_study(
        study_accession
    )
    mgnify_study.refresh_from_db()
    logger.info(f"MGnify study is {mgnify_study.accession}: {mgnify_study.title}")

    read_runs = get_study_readruns_from_ena(
        ena_study.accession,
        limit=10000,
        filter_library_strategy=EMG_CONFIG.amplicon_pipeline.amplicon_library_strategy,
        extra_cache_hash=ena_study.fetched_at.isoformat(),  # if ENA study is deleted/updated, the cache should be invalidated
    )
    logger.info(f"Returned {len(read_runs)} run from ENA portal API")

    # get or create Analysis for runs
    create_analyses(
        mgnify_study,
        for_experiment_type=analyses.models.WithExperimentTypeModel.ExperimentTypes.AMPLICON,
        pipeline=analyses.models.Analysis.PipelineVersions.v6,
    )
    analyses_to_attempt = get_analyses_to_attempt(
        mgnify_study,
        for_experiment_type=analyses.models.WithExperimentTypeModel.ExperimentTypes.AMPLICON,
    )

    # Work on chunks of 20 readruns at a time
    # Doing so means we don't use our entire cluster allocation for this study
    chunked_runs = chunk_list(
        analyses_to_attempt, EMG_CONFIG.amplicon_pipeline.samplesheet_chunk_size
    )
    for analyses_chunk in chunked_runs:
        # launch jobs for all analyses in this chunk in a single flow
        logger.info(
            f"Working on amplicon analyses: {analyses_chunk[0]}-{analyses_chunk[-1]}"
        )
        run_amplicon_pipeline_in_chunks(mgnify_study, analyses_chunk)
