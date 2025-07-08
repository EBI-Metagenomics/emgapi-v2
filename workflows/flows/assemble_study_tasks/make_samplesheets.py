import csv
from collections import defaultdict
from pathlib import Path
from textwrap import dedent as _
from typing import List, Union

from prefect import task
from prefect.artifacts import create_table_artifact
from prefect.cache_policies import DEFAULT
from prefect.tasks import task_input_hash

from activate_django_first import EMG_CONFIG

from workflows.flows.assemble_study_tasks.get_assemblies_to_attempt import (
    get_assemblies_to_attempt,
)

import analyses.models
from workflows.ena_utils.ena_file_fetching import convert_ena_ftp_to_fire_fastq
from workflows.ena_utils.ena_api_requests import SINGLE_END_LIBRARY_LAYOUT
from workflows.nextflow_utils.samplesheets import (
    SamplesheetColumnSource,
    queryset_hash,
    queryset_to_samplesheet,
)
from workflows.prefect_utils.analyses_models_helpers import chunk_list
from workflows.views import encode_samplesheet_path
from workflows.flows.assemble_study_tasks.assemble_samplesheets import (
    get_reference_genome,
)


@task(
    cache_key_fn=task_input_hash,
)
def make_samplesheet(
    mgnify_study: analyses.models.Study,
    assembly_ids: List[Union[str, int]],
    assembler: analyses.models.Assembler,
) -> (Path, str):
    """Generate a samplesheet for assemblies in a study.

    Creates a CSV samplesheet containing information about assemblies for processing
    with configuration based on the study, assembler and biome specifications.

    :param mgnify_study: The MGnify study containing the assemblies
    :param assembly_ids: List of assembly IDs to include in the samplesheet
    :param assembler: The assembler to be used for processing
    :return: A tuple containing the path to the generated samplesheet CSV file and a hash string generated from the assembly IDs
    """
    assemblies = analyses.models.Assembly.objects.select_related("run").filter(
        id__in=assembly_ids
    )

    ss_hash = queryset_hash(assemblies, "id")

    memory = get_memory_for_assembler(mgnify_study.biome, assembler)

    # Get contaminant reference genome if biome is found
    contaminant_reference = get_reference_genome(mgnify_study)

    sample_sheet_tsv = queryset_to_samplesheet(
        queryset=assemblies,
        filename=Path(EMG_CONFIG.slurm.default_workdir)
        / Path(
            f"{mgnify_study.ena_study.accession}_samplesheet_miassembler_{ss_hash}.csv"
        ),
        column_map={
            "study_accession": SamplesheetColumnSource(
                lookup_string="ena_study__accession"
            ),
            "reads_accession": SamplesheetColumnSource(
                lookup_string="run__ena_accessions",
                renderer=lambda accessions: accessions[0],
            ),
            "fastq_1": SamplesheetColumnSource(
                lookup_string=f"run__metadata__{analyses.models.Run.CommonMetadataKeys.FASTQ_FTPS}",
                renderer=lambda ftps: convert_ena_ftp_to_fire_fastq(ftps[0]),
            ),
            "fastq_2": SamplesheetColumnSource(
                lookup_string=f"run__metadata__{analyses.models.Run.CommonMetadataKeys.FASTQ_FTPS}",
                renderer=lambda ftps: (
                    convert_ena_ftp_to_fire_fastq(ftps[1]) if len(ftps) > 1 else ""
                ),
            ),
            "library_strategy": SamplesheetColumnSource(
                lookup_string="run__experiment_type",
                renderer=EXPERIMENT_TYPES_TO_MIASSEMBLER_LIBRARY_STRATEGY.get,
            ),
            "library_layout": SamplesheetColumnSource(
                lookup_string="run__metadata",
                renderer=lambda metadata: str(
                    metadata.get(
                        analyses.models.Run.CommonMetadataKeys.INFERRED_LIBRARY_LAYOUT,
                        metadata.get(
                            analyses.models.Run.CommonMetadataKeys.LIBRARY_LAYOUT
                        ),
                    )
                ).lower(),
            ),
            "platform": SamplesheetColumnSource(
                lookup_string=f"run__metadata__{analyses.models.Run.CommonMetadataKeys.INSTRUMENT_PLATFORM}",
                renderer=lambda platform: {
                    analyses.models.Run.InstrumentPlatformKeys.PACBIO_SMRT: "pb",
                    analyses.models.Run.InstrumentPlatformKeys.OXFORD_NANOPORE: "ont",
                    analyses.models.Run.InstrumentPlatformKeys.ION_TORRENT: "iontorrent",
                }.get(platform, str(platform).lower()),
            ),
            "assembler": SamplesheetColumnSource(
                # PACBIO_SMRT and OXFORD_NANOPORE - flye
                # SE platform ION_TORRENT         - spades
                # other SE                        - megahit
                # all the rest (mostly illumina)  - metaspades
                pass_whole_object=True,
                renderer=lambda assembly: (
                    analyses.models.Assembler.FLYE
                    if assembly.run.metadata_preferring_inferred.get(
                        analyses.models.Run.CommonMetadataKeys.INSTRUMENT_PLATFORM
                    )
                    in {
                        analyses.models.Run.InstrumentPlatformKeys.PACBIO_SMRT,
                        analyses.models.Run.InstrumentPlatformKeys.OXFORD_NANOPORE,
                    }
                    else (
                        analyses.models.Assembler.SPADES
                        if assembly.run.metadata_preferring_inferred.get(
                            analyses.models.Run.CommonMetadataKeys.LIBRARY_LAYOUT
                        )
                        == SINGLE_END_LIBRARY_LAYOUT
                        and assembly.run.metadata_preferring_inferred.get(
                            analyses.models.Run.CommonMetadataKeys.INSTRUMENT_PLATFORM
                        )
                        == analyses.models.Run.InstrumentPlatformKeys.ION_TORRENT
                        else (
                            analyses.models.Assembler.MEGAHIT
                            if assembly.run.metadata_preferring_inferred.get(
                                analyses.models.Run.CommonMetadataKeys.LIBRARY_LAYOUT
                            )
                            == SINGLE_END_LIBRARY_LAYOUT
                            else assembler.name.lower()
                        )
                    )
                ),
            ),
            "assembly_memory": str(memory),
            "contaminant_reference": contaminant_reference or "",
            # The following 2 fields are needed in the sampleshseet, but the production setup of the pipeline
            # sets these for the whole samplesheet. Also, if the human_reference global parameter and human_reference
            # in the samplesheet are empty, the pipeline will fail (the user needs to provide a
            # skip_human_decontamination flag).
            # Reference doc -> https://github.com/EBI-Metagenomics/miassembler?tab=readme-ov-file#usage
            "human_reference": "",
            "phix_reference": "",
        },
        bludgeon=True,
    )

    with open(sample_sheet_tsv) as f:
        csv_reader = csv.DictReader(f, delimiter=",")
        table = list(csv_reader)

    create_table_artifact(
        key="miassembler-initial-sample-sheet",
        table=table,
        description=_(
            f"""\
            Sample sheet created for run of MIAssembler.
            Saved to `{sample_sheet_tsv}`
            **Warning!** This table is the *initial* content of the samplesheet, when it was first made. Any edits made since are not shown here.
            [Edit it]({EMG_CONFIG.service_urls.app_root}/workflows/edit-samplesheet/fetch/{encode_samplesheet_path(sample_sheet_tsv)})
            """
        ),
    )
    return sample_sheet_tsv, ss_hash


@task(cache_policy=DEFAULT, persist_result=True)
def make_samplesheets_for_runs_to_assemble(
    mgnify_study_accession: str,
    assembler: analyses.models.Assembler,
    chunk_size: int = 10,
) -> list[tuple[Path, str]]:
    """Generate samplesheets for assemblies in a study for processing by the specified assembler.

    The biome is used to determine the amount of memory to allocate and for the decontamination step.
    This function handles the preparation of samplesheets based on the assemblies in a study
    and divides the assemblies into manageable chunks for processing. It checks for any
    conflicts in privacy settings of the assemblies with the study and ensures smooth
    integration with the assembly workflow.

    :param mgnify_study: The study containing assemblies to be processed
    :param assembler: The assembler software or framework used for processing
    :param chunk_size: The number of assemblies to include in each chunk, defaults to 10
    :return: A list of tuples, each containing the file path to a samplesheet and its associated identifier
    :raises ValueError: If any assemblies within the study have a conflicting privacy state compared to the study itself
    """
    mgnify_study = analyses.models.Study.objects.get(accession=mgnify_study_accession)
    if mgnify_study.assemblies_reads.exclude(
        is_private=mgnify_study.is_private
    ).exists():
        # This shouldn't happen, but in case it does - we should raise an error now instead of passing
        # impossible mixed publicity parameters to mi-assembler.
        raise ValueError(
            f"Study {mgnify_study} has assemblies whose privacy state does not match study."
        )

    assemblies_to_attempt = get_assemblies_to_attempt(mgnify_study)
    chunked_assemblies = chunk_list(assemblies_to_attempt, chunk_size)

    sheets = [
        make_samplesheet(mgnify_study, assembly_chunk, assembler)
        for assembly_chunk in chunked_assemblies
    ]
    return sheets


@task()
def get_memory_for_assembler(
    biome: analyses.models.Biome,
    assembler: analyses.models.Assembler,
):
    assembler_heuristics = analyses.models.ComputeResourceHeuristic.objects.filter(
        process=analyses.models.ComputeResourceHeuristic.ProcessTypes.ASSEMBLY,
        assembler=assembler,
    )

    # ascend the biome hierarchy to find a memory heuristic
    for biome_to_try in biome.ancestors().reverse():
        heuristic = assembler_heuristics.filter(biome=biome_to_try).first()
        if heuristic:
            return heuristic.memory_gb


EXPERIMENT_TYPES_TO_MIASSEMBLER_LIBRARY_STRATEGY = defaultdict(
    lambda: "other",
    **{
        analyses.models.Run.ExperimentTypes.METAGENOMIC: "metagenomic",
        analyses.models.Run.ExperimentTypes.METATRANSCRIPTOMIC: "metatranscriptomic",
    },
)
