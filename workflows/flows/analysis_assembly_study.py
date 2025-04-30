from textwrap import dedent as _
from typing import Optional, List

from prefect import flow, get_run_logger, suspend_flow_run
from prefect.input import RunInput
from pydantic import Field

from activate_django_first import EMG_CONFIG

import analyses.models
import ena.models
from workflows.ena_utils.ena_api_requests import (
    get_study_from_ena,
    get_study_assemblies_from_ena,
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
                        **Assembly Abalysis V6**
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

    # Make a top-level dir for this flow-run. May need to be cached? Does flow run ID change on retries?

    # Create analyses objects

    # Make samplesheets. Should be a cached task with persisted results for duration of this flow. Retries should NOT change samplesheets, unlike other flows.

    # Determine which samplesheets need to be executed. That means stuff below here needs to be in a subflow with retries that only succeeds if everything is in a terminal state.
    #   create samplesheet dir if not exist. should be hash of samplesheet content as well, in case it is edited.
    #   run pipeline in cluster job. reattach etc should work and be tested.
    #   grab success/fail lists.
    #   throw error if whole pipeline crashes. retry logic on outer flow should be aware of this and only retry if a sensible exception is raised that we believe may work in future.
    #   throw terminal error if whole pipeline crashes and a retry unlikely to work.
    #   on success set analysis dirs and import results
    #   run study summary generator on chunk

    # run summary merger
    # set study features flags
    # fire notification event
