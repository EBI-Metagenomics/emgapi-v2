from pathlib import Path

from workflows.data_io_utils.filenames import accession_prefix_separated_dir_path

MIASSEMBLER_ACCESSION_PREFIX_LENGTH = 7


def miassembler_run_output_dir(
    nextflow_outdir: Path | str,
    study_accession: str,
    run_accession: str,
) -> Path:
    """
    Build the MIAssembler output directory for one study/run pair.
    """
    return (
        Path(nextflow_outdir)
        / accession_prefix_separated_dir_path(
            study_accession, MIASSEMBLER_ACCESSION_PREFIX_LENGTH
        )
        / accession_prefix_separated_dir_path(
            run_accession, MIASSEMBLER_ACCESSION_PREFIX_LENGTH
        )
    )
