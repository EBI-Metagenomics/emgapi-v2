from enum import Enum
from textwrap import dedent
from typing import Optional, List

from django.urls import reverse_lazy
from prefect import flow, get_run_logger, suspend_flow_run
from prefect.events import emit_event
from prefect.input import RunInput
from prefect.runtime import flow_run, deployment
from pydantic import Field
from pydantic_core import PydanticUndefined

from workflows.views import encode_samplesheet_path

from emgapiv2.enum_utils import FutureStrEnum

from activate_django_first import EMG_CONFIG

import analyses.models
import ena.models
from workflows.ena_utils.ena_api_requests import (
    get_study_from_ena,
    get_study_readruns_from_ena,
    ENALibraryStrategyPolicy,
)
from workflows.ena_utils.webin_owner_utils import validate_and_set_webin_owner
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
from workflows.prefect_utils.analyses_models_helpers import (
    get_users_as_choices,
    add_study_watchers,
)


class AssemblerChoices(FutureStrEnum):
    # IDEA: it would be nice to sniff this from the pipeline schema
    pipeline_default = "pipeline_default"
    megahit = "megahit"
    metaspades = "metaspades"
    spades = "spades"
    flye = "flye"


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
)
def assemble_study(
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
    ena_study = ena.models.Study.objects.get_ena_study(accession)
    if not ena_study:
        ena_study = get_study_from_ena(accession)
        ena_study.refresh_from_db()
    logger.info(f"ENA Study is {ena_study.accession}: {ena_study.title}")

    # Get a MGnify Study object for this ENA Study
    mgnify_study: analyses.models.Study = (
        analyses.models.Study.objects.get_or_create_for_ena_study(accession)
    )

    logger.info(f"MGnify study is {mgnify_study.accession}: {mgnify_study.title}.")

    if mgnify_study.is_private:
        logger.info(f"{mgnify_study} is a private study.")

    read_runs = get_study_readruns_from_ena(
        ena_study.accession,
        limit=5000,
    )
    logger.info(f"Have {len(read_runs)} from ENA portal API")

    # define this within flow because it dynamically creates options from DB.
    BiomeChoices = get_biomes_as_choices()
    UserChoices = get_users_as_choices()

    class AssembleStudyInput(RunInput):
        biome: BiomeChoices = Field(
            default=(
                getattr(BiomeChoices, str(mgnify_study.biome.path))
                if mgnify_study.biome
                else PydanticUndefined
            )
        )
        assembler: AssemblerChoices = Field(
            ..., description="Default picked by run data type, unless overridden here."
        )
        watchers: Optional[List[UserChoices]] = Field(
            None,
            description="Admin users watching this study will get status notifications.",
        )
        webin_owner: Optional[str] = Field(
            default=(
                mgnify_study.webin_submitter if mgnify_study.webin_submitter else None
            ),
            description="Webin ID of study owner, if data is private. Can be left as None, if public.",
        )
        library_strategy_policy: ENALibraryStrategyPolicy = Field(
            ENALibraryStrategyPolicy.ONLY_IF_CORRECT_IN_ENA,
            description="Optionally treat read-runs with incorrect library strategy metadata as assemblable.",
        )
        wait_for_samplesheet_editing: bool = Field(
            False,
            description="If True, the execution will be suspended after the samplesheets are created, to allow editing.",
        )

    assemble_study_input: AssembleStudyInput = suspend_flow_run(
        wait_for_input=AssembleStudyInput.with_initial_data(
            assembler=AssemblerChoices.pipeline_default,
            description=dedent(
                f"""\
                **MI-Assembler**
                This will assemble all {len(read_runs)} read-runs of study {ena_study.accession} \
                using [MI-Assembler](https://www.github.com/ebi-metagenomics/mi-assembler).

                **Biome tagger**
                Please select a Biome for the entire study \
                [{ena_study.accession}: {ena_study.title}](https://www.ebi.ac.uk/ena/browser/view/{ena_study.accession}).

                The Biome is also used to guess how much memory is needed to assemble this study. \
                Guesses are determined by `ComputeResourceHeuristics`, \
                (can be edited in the [admin panel]({EMG_CONFIG.service_urls.app_root}/{reverse_lazy("admin:index")}).

                **Webin owner**
                If the study is private, the webin account owner is needed so that assemblies can be brokered into \
                the reads study they own.
                """
            ),
        )
    )

    validate_and_set_webin_owner(ena_study, assemble_study_input.webin_owner)
    mgnify_study.refresh_from_db()

    if assemble_study_input.watchers:
        add_study_watchers(mgnify_study, assemble_study_input.watchers)

    biome = analyses.models.Biome.objects.get(path=assemble_study_input.biome.name)

    mgnify_study.biome = biome
    mgnify_study.save()

    logger.info(f"MGnify study {mgnify_study.accession} has biome {biome.path}.")

    logger.info(f"Using assembler: {assemble_study_input.assembler}")
    assembler_name = assemble_study_input.assembler
    if assembler_name == AssemblerChoices.pipeline_default:
        assembler_name = analyses.models.Assembler.assembler_default

    assembler = (
        analyses.models.Assembler.objects.filter(name__iexact=assembler_name)
        .order_by("-version")
        .first()
    )
    # assumes latest version...

    get_or_create_assemblies_for_runs(
        mgnify_study.accession,
        read_runs,
        library_strategy_policy=assemble_study_input.library_strategy_policy,
    )
    samplesheets = make_samplesheets_for_runs_to_assemble(
        mgnify_study.accession, assembler
    )

    # If allow_samplesheet_editing is True, suspend the flow to allow editing
    if assemble_study_input.wait_for_samplesheet_editing and samplesheets:
        edit_urls = ""
        for samplesheet_path, _ in samplesheets:
            edit_urls += f"* [Edit samplesheet {samplesheet_path.name}]({EMG_CONFIG.service_urls.app_root}/workflows/edit-samplesheet/fetch/{encode_samplesheet_path(samplesheet_path)})\n"

        suspend_message = dedent(
            f"""\
            ## Samplesheets are ready for editing

            The following samplesheets have been created and are ready for editing:

            {{EDIT_URLS}}

            **Please edit the samplesheets as needed and then resume ("Submit") this flow.**
            The flow will fail after {EMG_CONFIG.assembler.suspend_timeout_for_editing_samplesheets_secs / 3600} hours if not resumed.
            """
        ).format(EDIT_URLS=edit_urls)

        class ResumeAfterSamplesheetEditingInput(RunInput):
            okay: bool = Field(
                True, description="Is a llama your favourite animal?"
            )  # fake input to make prefect suspend work

        suspend_flow_run(
            timeout=EMG_CONFIG.assembler.suspend_timeout_for_editing_samplesheets_secs,
            wait_for_input=ResumeAfterSamplesheetEditingInput.with_initial_data(
                description=suspend_message
            ),
        )

    logger.info("Flow resumed after samplesheet editing")

    for samplesheet_path, samplesheet_hash in samplesheets:
        logger.info(f"Will run assembler for samplesheet {samplesheet_path.name}")
        run_assembler_for_samplesheet(
            mgnify_study,
            samplesheet_path,
            samplesheet_hash,
        )

    if upload:
        upload_assemblies(mgnify_study, dry_run=use_ena_dropbox_dev)

    emit_event(
        event="flow.assembly.finished",
        resource={"prefect.resource.id": f"prefect.flow-run.{flow_run.id}"},
        related=[
            {
                "prefect.resource.id": f"prefect.deployment.{deployment.id}",
                "prefect.resource.role": "deployment",
            }
        ],
        payload={
            "successful": mgnify_study.assemblies_reads.filter_by_statuses(
                [analyses.models.Assembly.AssemblyStates.ASSEMBLY_COMPLETED]
            ).count(),
            "failed": mgnify_study.assemblies_reads.filter_by_statuses(
                [analyses.models.Assembly.AssemblyStates.ASSEMBLY_FAILED]
            ).count(),
            "uploaded": mgnify_study.assemblies_reads.filter_by_statuses(
                [analyses.models.Assembly.AssemblyStates.ASSEMBLY_UPLOADED]
            ).count(),
            "total": mgnify_study.assemblies_reads.count(),
            "study_watchers": [
                watcher.username for watcher in mgnify_study.watchers.all()
            ],
        },
    )
