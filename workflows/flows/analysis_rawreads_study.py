from typing import Optional, List

from prefect import flow, get_run_logger, suspend_flow_run
from prefect.events import emit_event
from prefect.input import RunInput
from prefect.runtime import flow_run, deployment
from pydantic import Field

from activate_django_first import EMG_CONFIG
from workflows.flows.analyse_study_tasks.copy_v6_pipeline_results import (
    copy_v6_study_summaries,
)

from workflows.flows.analyse_study_tasks.create_analyses import create_analyses
from workflows.flows.analyse_study_tasks.get_analyses_to_attempt import (
    get_analyses_to_attempt,
)
from workflows.flows.analyse_study_tasks.run_rawreads_pipeline_via_samplesheet import (
    run_rawreads_pipeline_via_samplesheet,
)

import analyses.models
import ena.models
from workflows.ena_utils.ena_api_requests import (
    get_study_from_ena,
    get_study_readruns_from_ena,
    library_strategy_policy_to_filter,
    ENALibraryStrategyPolicy,
)
from workflows.flows.analyse_study_tasks.shared.study_summary import (
    merge_study_summaries,
    add_rawreads_study_summaries_to_downloads,
)
from workflows.flows.assemble_study import get_biomes_as_choices
from workflows.prefect_utils.analyses_models_helpers import (
    chunk_list,
    get_users_as_choices,
    add_study_watchers,
)

_METAGENOMIC = "WGS"


@flow(
    name="Run analysis pipeline-v6 on raw-reads WGS study",
    log_prints=True,
    flow_run_name="Analyse raw-reads: {study_accession}",
)
def analysis_rawreads_study(study_accession: str):
    """
    Get a study from ENA, and input it to MGnify.
    Kick off raw-reads-v6 pipeline.
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
        raise_on_empty=False,
        filter_library_strategy=library_strategy_policy_to_filter(
            _METAGENOMIC, policy=ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA
        ),
    )
    logger.info(f"Returned {len(read_runs)} runs from ENA portal API")

    BiomeChoices = get_biomes_as_choices()
    UserChoices = get_users_as_choices()

    class AnalyseStudyInput(RunInput):
        biome: BiomeChoices
        watchers: Optional[List[UserChoices]] = Field(
            None,
            description="Admin users watching this study will get status notifications.",
        )
        library_strategy_policy: ENALibraryStrategyPolicy = Field(
            ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA,
            description="Optionally treat read-runs with incorrect library strategy metadata as raw-reads.",
        )

    analyse_study_input: AnalyseStudyInput = suspend_flow_run(
        wait_for_input=AnalyseStudyInput.with_initial_data(
            description=(
                f"""\
                **Raw-Reads V6**
                This will analyse all {len(read_runs)} read-runs of study {ena_study.accession} \
                using [Raw Reads Pipeline V6](https://github.com/ebi-metagenomics/raw-reads-analysis-pipeline).

                **Read-run filtering**
                If you expected more than {len(read_runs)} read-runs, you can add other (incorrect) \
                library strategies to the read-runs filter by changing the Library Strategy Policy.

                **Biome tagger**
                Please select a Biome for the entire study \
                [{ena_study.accession}: {ena_study.title}](https://www.ebi.ac.uk/ena/browser/view/{ena_study.accession}).
                """
            )
        )
    )

    if (
        analyse_study_input.library_strategy_policy
        and not analyse_study_input.library_strategy_policy
        == ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA
    ):
        read_runs = get_study_readruns_from_ena(
            ena_study.accession,
            limit=10000,
            raise_on_empty=True,
            filter_library_strategy=library_strategy_policy_to_filter(
                _METAGENOMIC, policy=analyse_study_input.library_strategy_policy
            ),
        )
        logger.info(
            f"Using policy {analyse_study_input.library_strategy_policy}, "
            f"now returned {len(read_runs)} run from ENA portal API."
        )

    biome = analyses.models.Biome.objects.get(path=analyse_study_input.biome.name)
    mgnify_study.biome = biome
    mgnify_study.save()
    logger.info(f"MGnify study {mgnify_study.accession} has biome {biome.path}.")

    if analyse_study_input.watchers:
        add_study_watchers(mgnify_study, analyse_study_input.watchers)

    # get or create Analysis for runs
    create_analyses(
        mgnify_study,
        for_experiment_type=analyses.models.WithExperimentTypeModel.ExperimentTypes.METAGENOMIC,
        pipeline=analyses.models.Analysis.PipelineVersions.v6,
        ena_library_strategy_policy=analyse_study_input.library_strategy_policy,
    )
    analyses_to_attempt = get_analyses_to_attempt(
        mgnify_study,
        for_experiment_type=analyses.models.WithExperimentTypeModel.ExperimentTypes.METAGENOMIC,
        ena_library_strategy_policy=analyse_study_input.library_strategy_policy,
    )

    # Work on chunks of 20 readruns at a time
    # Doing so means we don't use our entire cluster allocation for this study
    chunked_runs = chunk_list(
        analyses_to_attempt, EMG_CONFIG.rawreads_pipeline.samplesheet_chunk_size
    )
    for analyses_chunk in chunked_runs:
        # launch jobs for all analyses in this chunk in a single flow
        logger.info(
            f"Working on raw-reads analyses: {analyses_chunk[0]}-{analyses_chunk[-1]}"
        )
        run_rawreads_pipeline_via_samplesheet(mgnify_study, analyses_chunk)

    merge_study_summaries(
        mgnify_study.accession,
        cleanup_partials=not EMG_CONFIG.rawreads_pipeline.keep_study_summary_partials,
        analysis_type="rawreads",
    )
    add_rawreads_study_summaries_to_downloads(mgnify_study.accession)
    copy_v6_study_summaries(mgnify_study.accession)

    mgnify_study.refresh_from_db()
    mgnify_study.features.has_v6_analyses = mgnify_study.analyses.filter(
        pipeline_version=analyses.models.Analysis.PipelineVersions.v6, is_ready=True
    ).exists()
    mgnify_study.save()

    emit_event(
        event="flow.analysis.finished",
        resource={"prefect.resource.id": f"prefect.flow-run.{flow_run.id}"},
        related=[
            {
                "prefect.resource.id": f"prefect.deployment.{deployment.id}",
                "prefect.resource.role": "deployment",
            }
        ],
        payload={
            "successful": mgnify_study.analyses.filter_by_statuses(
                [analyses.models.Analysis.AnalysisStates.ANALYSIS_COMPLETED]
            ).count(),
            "failed": mgnify_study.analyses.filter_by_statuses(
                [analyses.models.Analysis.AnalysisStates.ANALYSIS_FAILED]
            ).count()
            + mgnify_study.analyses.filter_by_statuses(
                [analyses.models.Analysis.AnalysisStates.ANALYSIS_QC_FAILED]
            ).count()
            + mgnify_study.analyses.filter_by_statuses(
                [
                    analyses.models.Analysis.AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED
                ]
            ).count(),
            "imported": mgnify_study.analyses.filter_by_statuses(
                [analyses.models.Analysis.AnalysisStates.ANALYSIS_ANNOTATIONS_IMPORTED]
            ).count(),
            "total": mgnify_study.analyses.count(),
            "study_watchers": [
                watcher.username for watcher in mgnify_study.watchers.all()
            ],
        },
    )
