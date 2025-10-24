from pathlib import Path
from textwrap import dedent as _
from typing import Optional, List

from prefect import flow, get_run_logger, suspend_flow_run
from prefect.input import RunInput
from pydantic import Field

import analyses.models
import ena.models
import workflows.models
from activate_django_first import EMG_CONFIG
from workflows.ena_utils.ena_api_requests import (
    get_study_from_ena,
    get_study_assemblies_from_ena,
)
from workflows.flows.analyse_study_tasks.shared.copy_v6_pipeline_results import (
    copy_v6_study_summaries,
    copy_assembly_batch_results,
)
from workflows.flows.analyse_study_tasks.shared.study_summary import (
    merge_assembly_study_summaries,
    add_study_summaries_to_downloads,
)
from workflows.flows.analysis.assembly.flows.run_assembly_analysis_pipeline_batch import (
    run_assembly_analysis_pipeline_batch,
)
from workflows.flows.analysis.assembly.tasks.create_analyses_for_assemblies import (
    create_analyses_for_assemblies,
)
from workflows.flows.assemble_study import get_biomes_as_choices
from workflows.prefect_utils.analyses_models_helpers import (
    get_users_as_choices,
    add_study_watchers,
)


@flow(
    name="Run assembly analysis v6 pipeline on a study",
    flow_run_name="Analyse assembly: {study_accession}",
)
def analysis_assembly_study(study_accession: str, workspace_dir: str = None):
    """
    Get a study from ENA (or MGnify), and run assembly-v6 pipeline on its read-runs.

    This then chains the Assembly Analysis Pipeline, VIRIfy and the Mobilome Annotation Pipeline using
    batches of assemblies to analise.

    It keeps track of the batches and their statuses.

    :param study_accession: e.g. PRJ or ERP accession
    :param workspace_dir: Optional path for the workspace dir.
    """
    logger = get_run_logger()

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
    assemblies_accessions: list[str] = get_study_assemblies_from_ena(
        ena_study.accession,
        limit=500,  # TODO: this should be a config value
    )
    logger.info(f"Returned {len(assemblies_accessions)} assemblies from ENA portal API")

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
                        This will analyse all {len(assemblies_accessions)} assemblies of study {ena_study.accession} \
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

    # Create analysis objects for all assemblies
    create_analyses_for_assemblies(
        mgnify_study.id,
        assemblies_accessions,
        pipeline=analyses.models.Analysis.PipelineVersions.v6,
    )

    # Create batches for study analyses (handles chunking, duplicate checking, safety caps)
    batches = (
        workflows.models.AssemblyAnalysisBatch.objects.get_or_create_batches_for_study(
            study=mgnify_study,
            pipeline=analyses.models.Analysis.PipelineVersions.v6,
            max_analyses=EMG_CONFIG.assembly_analysis_pipeline.max_analyses_per_study,
            workspace_dir=Path(workspace_dir) if workspace_dir else None,
        )
    )

    logger.info(f"Created {len(batches)} batches for study {mgnify_study.accession}")

    logger.info(f"Running {len(batches)} batches...")
    for batch in batches:
        logger.info(
            f"Running batch {str(batch.id)[:8]} with {batch.total_analyses} analyses"
        )
        run_assembly_analysis_pipeline_batch(batch.id)

    #######################################
    # === Sync results for each batch === #
    #######################################
    for batch in batches:
        copy_assembly_batch_results(batch.id)

    ###############################################
    # === Study summary merging and uploading === #
    ###############################################
    merge_assembly_study_summaries(
        mgnify_study.accession,
        cleanup_partials=True,
    )

    add_study_summaries_to_downloads(mgnify_study.accession)

    copy_v6_study_summaries(mgnify_study.accession)

    mgnify_study.refresh_from_db()

    # Sanity check
    mgnify_study.features.has_v6_analyses = mgnify_study.analyses.filter(
        pipeline_version=analyses.models.Analysis.PipelineVersions.v6, is_ready=True
    ).exists()

    mgnify_study.save()

    # # TODO: review these... for the time being use the admin panel
    # # Build event payload
    # payload = { }
    # events.emit_event( )
