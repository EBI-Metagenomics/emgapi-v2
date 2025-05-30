import csv
from collections import defaultdict
from pathlib import Path
from textwrap import dedent as _
from typing import List, Union

from prefect import task
from prefect.artifacts import create_table_artifact
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


@task(
    cache_key_fn=task_input_hash,
)
def make_samplesheet(
    mgnify_study: analyses.models.Study,
    assembly_ids: List[Union[str, int]],
    assembler: analyses.models.Assembler,
) -> (Path, str):
    assemblies = analyses.models.Assembly.objects.select_related("run").filter(
        id__in=assembly_ids
    )

    ss_hash = queryset_hash(assemblies, "id")

    memory = get_memory_for_assembler(mgnify_study.biome, assembler)

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
                lookup_string=f"run__metadata__{analyses.models.Run.CommonMetadataKeys.LIBRARY_LAYOUT}",
                renderer=lambda layout: str(layout).lower(),
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
                lookup_string=[
                    f"run__metadata__{analyses.models.Run.CommonMetadataKeys.LIBRARY_LAYOUT}",
                    f"run__metadata__{analyses.models.Run.CommonMetadataKeys.INSTRUMENT_PLATFORM}",
                ],
                # PACBIO_SMRT and OXFORD_NANOPORE - flye
                # SE platform ION_TORRENT         - spades
                # other SE                        - megahit
                # all the rest (mostly illumina)  - metaspades
                renderer=lambda layout, platform: (
                    analyses.models.Assembler.FLYE
                    if platform
                    in {
                        analyses.models.Run.InstrumentPlatformKeys.PACBIO_SMRT,
                        analyses.models.Run.InstrumentPlatformKeys.OXFORD_NANOPORE,
                    }
                    else (
                        analyses.models.Assembler.SPADES
                        if layout == SINGLE_END_LIBRARY_LAYOUT
                        and platform
                        == analyses.models.Run.InstrumentPlatformKeys.ION_TORRENT
                        else (
                            analyses.models.Assembler.MEGAHIT
                            if layout == SINGLE_END_LIBRARY_LAYOUT
                            else assembler.name.lower()
                        )
                    )
                ),
            ),
            "assembly_memory": SamplesheetColumnSource(
                lookup_string="id", renderer=lambda _: memory
            ),
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


@task(
    cache_key_fn=task_input_hash,
)
def make_samplesheets_for_runs_to_assemble(
    mgnify_study: analyses.models.Study,
    assembler: analyses.models.Assembler,
    chunk_size: int = 10,
) -> [(Path, str)]:

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
