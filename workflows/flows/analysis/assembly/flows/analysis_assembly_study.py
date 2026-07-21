from pathlib import Path
from textwrap import dedent as _
from typing import List, Optional, Type

from prefect import get_run_logger, suspend_flow_run
from prefect.deployments import run_deployment
from prefect.input import RunInput
from pydantic import Field

from activate_django_first import EMG_CONFIG

import analyses.models
import ena.models
import workflows.models
from workflows.ena_utils.ena_api_requests import (
    get_study_assemblies_from_ena,
    get_study_from_ena,
)
from workflows.ena_utils.ena_policies import (
    ENALibrarySourcePolicy,
    ENALibraryStrategyPolicy,
)
from workflows.ena_utils.webin_owner_utils import validate_and_set_webin_owner
from workflows.flows.analysis.assembly.tasks.create_analyses_for_assemblies import (
    create_analyses_for_assemblies,
)
from workflows.flows.assemble_study import get_biomes_as_choices
from workflows.prefect_utils.analyses_models_helpers import (
    add_study_watchers,
    get_users_as_choices,
)
from workflows.prefect_utils.flows_utils import django_db_flow as flow
from workflows.prefect_utils.input_helpers import ask_every_time_suspend_for_input_key


@flow(
    name="Run assembly analysis v6 pipeline on a study",
    flow_run_name="Analyse assembly: {study_accession}",
)
def analysis_assembly_study(
    study_accession: str,
    workspace_dir: str = EMG_CONFIG.slurm.default_workdir,
    contaminant_reference: Optional[str] = None,
):
    """
    Get a study from ENA (or MGnify), and run assembly analysis the assemblies of the study.

    This then chains the Assembly Analysis Pipeline, VIRIfy and the Mobilome Annotation Pipeline using
    batches of assemblies to analyse.

    It keeps track of the batches and their statuses.

    :param study_accession: e.g. PRJ or ERP accession
    :param workspace_dir: Path for the workspace dir. Defaults to the configured SLURM default workdir.
    :param contaminant_reference: Optional contaminant reference name to use for ASA decontamination. See: https://github.com/EBI-Metagenomics/assembly-analysis-pipeline/blob/main/docs/usage.md#samplesheet-input
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

    if mgnify_study.is_private:
        logger.info(f"{mgnify_study} is a private study.")

    # Get assemble-able runs
    assemblies_accessions: list[str] = get_study_assemblies_from_ena(
        ena_study.accession,
        limit=EMG_CONFIG.ena.portal_max_readruns_to_fetch,
        expected_experiment_type=analyses.models.Run.ExperimentTypes.METAGENOMIC,
    )
    logger.info(f"Returned {len(assemblies_accessions)} assemblies from ENA portal API")

    # Suspend if biome is needed, or if the study is private and has no webin submitter set yet
    needs_biome = not mgnify_study.biome
    needs_webin = mgnify_study.is_private and not ena_study.webin_submitter

    if needs_biome or needs_webin:
        BiomeChoices = get_biomes_as_choices()
        UserChoices = get_users_as_choices()

        class AnalyseStudyInput(RunInput):
            biome: BiomeChoices
            watchers: Optional[List[Type[UserChoices]]] = Field(
                None,
                description="Admin users watching this study will get status notifications.",
            )
            library_strategy_policy: ENALibraryStrategyPolicy = Field(
                ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA,
                description="Optionally treat assemblies with incorrect library strategy metadata as raw-reads.",
            )
            library_source_policy: ENALibrarySourcePolicy = Field(
                ENALibrarySourcePolicy.OVERRIDE_GENOMIC_IF_METAGENOMIC_SCIENTIFIC_NAME,
                description="How to handle the library source metadata (e.g. if it is GENOMIC instead of METAGENOMIC).",
            )
            webin_owner: Optional[str] = Field(
                None,
                description="Webin ID of study owner, if data is private. Can be left as None if public.",
            )

        initial_data = {}
        if not needs_biome:
            initial_data["biome"] = BiomeChoices[str(mgnify_study.biome.path)]

        analyse_study_input: AnalyseStudyInput = suspend_flow_run(
            wait_for_input=AnalyseStudyInput.with_initial_data(
                **initial_data,
                description=_(f"""\
                        **Assembly Analysis V6**
                        This will analyse all {len(assemblies_accessions)} assemblies of study {ena_study.accession} \
                        using [Assembly Analysis Pipeline V6](https://github.com/EBI-Metagenomics/assembly-analysis-pipeline).

                        {"**Biome tagger** Please select a Biome for the entire study " + f"[{ena_study.accession}: {ena_study.title}](https://www.ebi.ac.uk/ena/browser/view/{ena_study.accession})." if needs_biome else f"Biome is already set to {mgnify_study.biome} — change here if needed."}

                        {"**Webin owner** The study is private. Please provide the Webin account ID of the study owner so they can view their study." if needs_webin else ""}
                        """),
            ),
            timeout=EMG_CONFIG.slurm.default_flow_suspend_awaiting_input_timeout_secs,
            key=ask_every_time_suspend_for_input_key(),
        )

        biome = analyses.models.Biome.objects.get(path=analyse_study_input.biome.name)
        mgnify_study.biome = biome
        mgnify_study.save()
        logger.info(f"MGnify study {mgnify_study.accession} has biome {biome.path}.")

        if analyse_study_input.watchers:
            add_study_watchers(mgnify_study, analyse_study_input.watchers)

        validate_and_set_webin_owner(ena_study, analyse_study_input.webin_owner)
        mgnify_study.refresh_from_db()

        if (
            analyse_study_input.library_strategy_policy
            != ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA
            or analyse_study_input.library_source_policy
            != ENALibrarySourcePolicy.OVERRIDE_GENOMIC_IF_METAGENOMIC_SCIENTIFIC_NAME
        ):
            assemblies_accessions = get_study_assemblies_from_ena(
                ena_study.accession,
                limit=EMG_CONFIG.ena.portal_max_readruns_to_fetch,
                library_strategy_policy=analyse_study_input.library_strategy_policy,
                library_source_policy=analyse_study_input.library_source_policy,
                expected_experiment_type=analyses.models.Run.ExperimentTypes.METAGENOMIC,
            )
            logger.info(
                f"Using policies strategy:{analyse_study_input.library_strategy_policy} source:{analyse_study_input.library_source_policy}, "
                f"now returned {len(assemblies_accessions)} assemblies from ENA portal API."
            )
    else:
        logger.info(
            f"Biome {mgnify_study.biome} was already set for this study. If a change is needed, do so in the DB Admin Panel."
        )
        logger.info(
            f"MGnify study currently has watchers {mgnify_study.watchers.values_list('username', flat=True)}. Add more in the DB Admin Panel if needed."
        )

    if mgnify_study.is_private and not mgnify_study.webin_submitter:
        raise ValueError(
            f"Study {mgnify_study.accession} is private, but no webin owner was provided."
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
            workspace_dir=Path(workspace_dir),
            contaminant_reference=contaminant_reference,
        )
    )

    logger.info(f"Created {len(batches)} batches for study {mgnify_study.accession}")

    logger.info(f"Running {len(batches)} batches in parallel...")
    for batch in batches:
        logger.info(
            f"Running batch {str(batch.id)[:8]} with {batch.total_analyses} analyses"
        )
        run_deployment(
            name="run-assembly-batch/run_assembly_batch_deployment",
            parameters={"assembly_analyses_batch_id": batch.id},
            timeout=0,  # Timeout=0 means to run in the background and return immediately
        )

    logger.info(
        f"All {len(batches)} batches submitted successfully. "
        "Finalization will be triggered automatically when all batches complete."
    )
