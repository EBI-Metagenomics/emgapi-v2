from textwrap import dedent as _
from typing import Optional, List

from prefect import flow, get_run_logger, suspend_flow_run, events
from prefect.input import RunInput
from prefect.runtime import flow_run, deployment
from pydantic import Field

from activate_django_first import EMG_CONFIG

import analyses.models
import ena.models
from workflows.ena_utils.ena_api_requests import (
    get_study_from_ena,
    get_study_assemblies_from_ena,
)
from workflows.flows.analyse_study_tasks.copy_v6_pipeline_results import (
    copy_v6_study_summaries,
)
from workflows.flows.analyse_study_tasks.create_analyses_for_assemblies import (
    create_analyses_for_assemblies,
)
from workflows.flows.analyse_study_tasks.get_analyses_to_attempt import (
    get_analyses_to_attempt,
)
from workflows.flows.analyse_study_tasks.run_assembly_pipeline_via_samplesheet import (
    run_assembly_pipeline_via_samplesheet,
)
from workflows.flows.analyse_study_tasks.shared.study_summary import (
    merge_study_summaries,
    add_study_summaries_to_downloads,
)
from workflows.flows.assemble_study import get_biomes_as_choices
from workflows.prefect_utils.analyses_models_helpers import (
    chunk_list,
    get_users_as_choices,
    add_study_watchers,
)


@flow(
    name="Run assembly analysis v6 pipeline on a study",
    flow_run_name="Analyse assembly: {study_accession}",
)
def analysis_assembly_study(study_accession: str):
    """
    Get a study from ENA (or MGnify), and run assembly-v6 pipeline on its read-runs.
    :param study_accession: e.g. PRJ or ERP accession
    """
    logger = get_run_logger()
    EMG_CONFIG

    # Study fetching and creation
    ena_study = ena.models.Study.objects.get_ena_study(study_accession)
    if not ena_study:
        ena_study = get_study_from_ena(study_accession)

    ena_study.refresh_from_db()
    logger.info(f"ENA Study is {ena_study.accession}: {ena_study.title}")

    mgnify_study = analyses.models.Study.objects.get_or_create_for_ena_study(
        study_accession
    )
    mgnify_study.refresh_from_db()
    logger.info(f"MGnify study is {mgnify_study.accession}: {mgnify_study.title}")

    # Get assemble-able runs
    assemblies = get_study_assemblies_from_ena(
        ena_study.accession,
        limit=10000,
    )
    logger.info(f"Returned {len(assemblies)} assemblies from ENA portal API")

    # Check if biome-picker is needed
    if not mgnify_study.biome:
        BiomeChoices = get_biomes_as_choices()
        UserChoices = get_users_as_choices()

        class AnalyseStudyInput(RunInput):
            biome: BiomeChoices
            watchers: Optional[List[UserChoices]] = Field(
                None,
                description="Admin users watching this study will get status notifications.",
            )

        analyse_study_input: AnalyseStudyInput = suspend_flow_run(
            wait_for_input=AnalyseStudyInput.with_initial_data(
                description=_(
                    f"""\
                        **Assembly Analysis V6**
                        This will analyse all {len(assemblies)} assemblies of study {ena_study.accession} \
                        using [Assembly Analysis Pipeline V6](https://github.com/EBI-Metagenomics/assembly-analysis-pipeline).

                        **Biome tagger**
                        Please select a Biome for the entire study \
                        [{ena_study.accession}: {ena_study.title}](https://www.ebi.ac.uk/ena/browser/view/{ena_study.accession}).
                        """
                ),
            )
        )

        biome = analyses.models.Biome.objects.get(path=analyse_study_input.biome.name)
        mgnify_study.biome = biome
        mgnify_study.save()
        logger.info(f"MGnify study {mgnify_study.accession} has biome {biome.path}.")

        if analyse_study_input.watchers:
            add_study_watchers(mgnify_study, analyse_study_input.watchers)
    else:
        logger.info(
            f"Biome {mgnify_study.biome} was already set for this study. If an change is needed, do so in the DB Admin Panel."
        )
        logger.info(
            f"MGnify study currently has watchers {mgnify_study.watchers.values_list('username', flat=True)}. Add more in the DB Admin Panel if needed."
        )

    # Create analyses objects for all assemblies
    create_analyses_for_assemblies(
        mgnify_study,
        pipeline=analyses.models.Analysis.PipelineVersions.v6,
    )
    analyses_to_attempt = get_analyses_to_attempt(
        mgnify_study,
        for_experiment_type=analyses.models.WithExperimentTypeModel.ExperimentTypes.ASSEMBLY,
    )

    # Work on chunks of 20 assemblies at a time
    # Doing so means we don't use our entire cluster allocation for this study
    chunked_analyses = chunk_list(
        analyses_to_attempt,
        EMG_CONFIG.assembly_analysis_pipeline.samplesheet_chunk_size,
    )
    for analyses_chunk in chunked_analyses:
        # launch jobs for all analyses in this chunk in a single flow
        logger.info(
            f"Working on assembly analyses: {analyses_chunk[0]}-{analyses_chunk[-1]}"
        )
        run_assembly_pipeline_via_samplesheet(mgnify_study, analyses_chunk)

    merge_study_summaries(
        mgnify_study.accession,
        cleanup_partials=True,
        analysis_type="assembly",
    )
    add_study_summaries_to_downloads(mgnify_study.accession)
    copy_v6_study_summaries(mgnify_study.accession)

    mgnify_study.refresh_from_db()
    mgnify_study.features.has_v6_analyses = mgnify_study.analyses.filter(
        pipeline_version=analyses.models.Analysis.PipelineVersions.v6, is_ready=True
    ).exists()
    mgnify_study.save()

    events.emit_event(
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
