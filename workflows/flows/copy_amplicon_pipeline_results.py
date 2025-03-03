import django
from prefect import flow

from workflows.prefect_utils.datamovers import move_data

django.setup()

from analyses.models import Analysis
from workflows.data_io_utils.mgnify_v6_utils.amplicon import EMG_CONFIG


@flow(
    name="Copy Amplicon Pipeline Results",
    flow_run_name="Copy Amplicon Pipeline Results",
    log_prints=True,
)
def copy_amplicon_pipeline_results(analysis_accession: str):
    analysis = Analysis.objects.get(accession=analysis_accession)
    study = analysis.study
    run = analysis.run
    source = f"/nfs/production/rdf/metagenomics/results/{analysis.results_dir}"
    target = f"{EMG_CONFIG.slurm.ftp_results_dir}/{study.first_accession[:-3]}/{study.first_accession}/{run.first_accession[:-3]}/{run.first_accession}/{analysis.pipeline_version}/{analysis.experiment_type.lower()}"

    allowed_extensions = [
        "yml",
        "yaml",
        "txt",
        "tsv",
        "mseq",
        "html",
        "fa",
        "json",
        "gz",
        "fasta",
        "csv",
    ]
    find_pattern = (
        "\\( "
        + " -o ".join([f"-name '*.{ext}'" for ext in allowed_extensions])
        + " \\)"
    )

    command = (
        f"mkdir -p '{target}' && "
        f"cd '{source}' && "
        f"find . -type f {find_pattern} -print0 | "  # Use -print0 to handle filenames with spaces
        f"while IFS= read -r -d $'\\0' file; do "
        f'  targetdir="{target}/$(dirname "$file")" && '
        f'  mkdir -p "$targetdir" && '
        f'  cp "$file" "{target}/$file"; '
        f"done"
    )
    move_data(source, target, command)
