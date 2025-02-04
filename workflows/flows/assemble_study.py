from enum import Enum

import django
from asgiref.sync import sync_to_async
from django.conf import settings
from django.urls import reverse_lazy
from prefect import flow, get_run_logger, suspend_flow_run
from prefect.input import RunInput
from prefect.task_runners import SequentialTaskRunner

from emgapiv2.enum_utils import FutureStrEnum

django.setup()

import analyses.models
import ena.models
from workflows.ena_utils.ena_api_requests import (
    get_study_from_ena,
    get_study_readruns_from_ena,
)
from workflows.flows.assemble_study_tasks.assemble_samplesheets import (
    run_assembler_for_samplesheet,
)
from workflows.flows.assemble_study_tasks.get_assemblies_for_runs import (
    get_or_create_assemblies_for_runs,
)
from workflows.flows.assemble_study_tasks.make_samplesheets import (
    make_samplesheets_for_runs_to_assemble,
)
from workflows.flows.assemble_study_tasks.upload_assemblies import upload_assemblies
from workflows.prefect_utils.slack_notification import notify_via_slack

EMG_CONFIG = settings.EMG_CONFIG


class AssemblerChoices(FutureStrEnum):
    # IDEA: it would be nice to sniff this from the pipeline schema
    pipeline_default = "pipeline_default"
    megahit = "megahit"
    metaspades = "metaspades"
    spades = "spades"


def get_biomes_as_choices():
    # IDEA: move this one to a helper of some sorts
    biomes = {
        str(biome.path): biome.pretty_lineage
        for biome in analyses.models.Biome.objects.all()
    }
    BiomeChoices = Enum("BiomeChoices", biomes)
    return BiomeChoices


@flow(
    name="Assemble a study",
    flow_run_name="Assemble: {accession}",
    task_runner=SequentialTaskRunner,
)
async def assemble_study(
    accession: str, upload: bool = True, use_ena_dropbox_dev: bool = False
):
    """
    Get a study from ENA, and input it to MGnify.
    Kick off assembly pipeline.
    :param accession: Study accession e.g. PRJxxxxxx
    :param upload: Whether to upload the TPA study or not
    :param use_ena_dropbox_dev: Whether to use ENA wwwdev dropbox
    """
    logger = get_run_logger()

    # Create (or get) an ENA Study object, populating with metadata from ENA
    # Refresh from DB in case we get an old cached version.
    ena_study = await ena.models.Study.objects.get_ena_study(accession)
    if not ena_study:
        ena_study = await get_study_from_ena(accession)
        await ena_study.arefresh_from_db()
    logger.info(f"ENA Study is {ena_study.accession}: {ena_study.title}")

    # Get a MGnify Study object for this ENA Study
    mgnify_study = await analyses.models.Study.objects.get_or_create_for_ena_study(
        accession
    )

    logger.info(f"MGnify study is {mgnify_study.accession}: {mgnify_study.title}.")

    read_runs = get_study_readruns_from_ena(
        ena_study.accession,
        limit=5000,
        extra_cache_hash=ena_study.fetched_at.isoformat(),  # if ENA study is deleted/updated, the cache should be invalidated
    )
    logger.info(f"Have {len(read_runs)} from ENA portal API")

    # define this within flow because it dynamically creates options from DB.
    BiomeChoices = await sync_to_async(get_biomes_as_choices)()

    class BiomeAndAssemblerInput(RunInput):
        biome: BiomeChoices
        assembler: AssemblerChoices

    biome_and_assembler_input: BiomeAndAssemblerInput = await suspend_flow_run(
        wait_for_input=BiomeAndAssemblerInput.with_initial_data(
            assembler=AssemblerChoices.pipeline_default,
            description=f"""
**MI-Assembler**
This will assemble all {len(read_runs)} read-runs of study {ena_study.accession} \
using [MI-Assembler](https://www.github.com/ebi-metagenomics/mi-assembler).

Please pick which assembler tool to use (otherwise a default one will be picked depending on the data type).

**Biome tagger**
Please select a Biome for the entire study \
[{ena_study.accession}: {ena_study.title}](https://www.ebi.ac.uk/ena/browser/view/{ena_study.accession}).

The Biome is important metadata, and will also be used to guess how much memory is needed to assemble this study. \
These guesses are determined by the `ComputeResourceHeuristics`, \
which you can edit in the [admin panel]({EMG_CONFIG.service_urls.app_root}/{reverse_lazy("admin:index")}).
        """,
        )
    )

    biome = await analyses.models.Biome.objects.aget(
        path=biome_and_assembler_input.biome.name
    )

    mgnify_study.biome = biome
    await mgnify_study.asave()

    logger.info(f"MGnify study {mgnify_study.accession} has biome {biome.path}.")

    logger.info(f"Using assembler: {biome_and_assembler_input.assembler}")
    assembler_name = biome_and_assembler_input.assembler
    if assembler_name == AssemblerChoices.pipeline_default:
        assembler_name = analyses.models.Assembler.assembler_default

    assembler = (
        await analyses.models.Assembler.objects.filter(name__iexact=assembler_name)
        .order_by("-version")
        .afirst()
    )
    # assumes latest version...

    get_or_create_assemblies_for_runs(mgnify_study, read_runs)
    samplesheets = make_samplesheets_for_runs_to_assemble(mgnify_study, assembler)
    for samplesheet in samplesheets:
        run_assembler_for_samplesheet(
            mgnify_study,
            samplesheet,
            assembler,
        )
    await notify_via_slack(f"Assembly of {mgnify_study} / {accession} is finished")

    if upload:
        await upload_assemblies(mgnify_study, dry_run=use_ena_dropbox_dev)
