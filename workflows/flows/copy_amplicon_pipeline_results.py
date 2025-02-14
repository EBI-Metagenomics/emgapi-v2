from prefect import flow, get_run_logger
import django
from pathlib import Path
django.setup()

from analyses.models import Analysis
from workflows.data_io_utils.mgnify_v6_utils.amplicon import EMG_CONFIG
from workflows.prefect_utils.slurm_flow import move_data


@flow(
    name="Copy Amplicon Pipeline Results",
    flow_run_name="Copy Amplicon Pipeline Results",
    log_prints=True,
)
# async def copy_amplicon_pipeline_results(analysis: Analysis):
def copy_amplicon_pipeline_results(analysis_accession: str):
    analysis = Analysis.objects.get(accession=analysis_accession)
    # logger = get_run_logger()
    print(f"Copying Amplicon Pipeline Results for {analysis.accession}")
    print(f"Results dir: {analysis.results_dir}")

    print(f"FTP results dir: {EMG_CONFIG.slurm.ftp_results_dir}")

    print(f"Study first accession: {analysis.study.accession}")
    # move_data('/nfs/production/rdf/metagenomics/results', '/nfs/ftp/public/databases/metagenomics/mgnify_results',
    #           'cp -r')
    # move_data(f"/nfs/production/rdf/metagenomics/results/{analysis.results_dir}",
    #     f"{EMG_CONFIG.slurm.ftp_results_dir}/{analysis.study.accession[:4]}/{analysis.study.accession}",
    #     'cp -r'
    # )

    source = f"/nfs/production/rdf/metagenomics/results/{analysis.results_dir}"
    target = f"{EMG_CONFIG.slurm.ftp_results_dir}/{analysis.study.accession[:4]}/{analysis.study.accession}"

    # ALLOWED_EXTENSIONS = {
    #     '.yml',  # software_versions.yml
    #     '.txt', '.tsv',  # taxonomy and ASV files
    #     '.mseq',  # taxonomy files
    #     '.html',  # reports
    #     '.fa', '.fasta',  # sequence files
    #     '.gz',  # compressed fastq
    #     '.json',  # metadata and stats
    #     '.csv',  # QC files
    # }

    # ALLOWED_PATTERNS = {
    #     'pipeline_info': ['software_versions.yml'],
    #     'taxonomy-summary': ['*.txt', '*.tsv', '*.mseq', '*.html'],
    #     'sequence-categorisation': ['*.fa', '*.fasta', '*.deoverlapped'],
    #     'qc': ['*.html', '*.json', '*.tsv', '*.gz'],
    #     'primer-identification': ['*.tsv', '*.fasta', '*.json'],
    #     'asv': ['*.tsv', '*.fasta'],
    #     'amplified-region-inference': ['*.tsv', '*.txt'],
    #     '.': ['sample.txt','study_multiqc_report.html', 'qc_*.csv', 'primer_*.json', 'manifest.json']
    # }

    ALLOWED_PATTERNS = {
        '.': ['*']  # This will match any file in any directory
    }

    pattern_commands = []
    # pattern_commands = ["find . -type f"]
    for dir, patterns in ALLOWED_PATTERNS.items():
        patterns_str = " -o ".join([f"-name '{p}'" for p in patterns])
        if dir == '.':
            # pattern_commands.append(f"find . -maxdepth 1 \\( {patterns_str} \\) -type f")
            pattern_commands.append(f"find . \\( {patterns_str} \\) -type f")
        else:
            pattern_commands.append(f"find . -path '*/{dir}/*' \\( {patterns_str} \\) -type f")

    # Create a shell script that will be executed
    # full_command = (
    #     f"mkdir -p '{target}' && "
    #     f"cd '{source}' && "
    #     f"{{ {' ; '.join(pattern_commands)}; }} | "
    #     f"while IFS= read -r file; do "
    #     f"  mkdir -p \"$(dirname '{target}/$file')\" && "
    #     f"  cp \"$file\" \"{target}/$file\"; "
    #     f"done"
    # )

    full_command = (
        f"mkdir -p '{target}' && "
        f"cd '{source}' && "
        f"find . -type f | "
        f"while IFS= read -r file; do "
        f"  mkdir -p \"$(dirname '{target}/$file')\" && "
        f"  cp \"$file\" \"{target}/$file\"; "
        f"done"
    )


    # Create find command to list only allowed files
    # extension_pattern = " -o ".join([f"-name '*{ext}'" for ext in ALLOWED_EXTENSIONS])

    # Command to:
    # 1. Create target directory
    # 2. Find allowed files
    # 3. Create target directories
    # 4. Copy files preserving structure
    # full_command = f"""
    #        mkdir -p '{target}' && \
    #        cd '{source}' && \
    #        find . \( {extension_pattern} \) -type f | while read file; do
    #            mkdir -p "$(dirname '{target}/'$file)"
    #            cp "$file" "{target}/"$file
    #        done
    #    """


    move_command = f"mkdir -p '{Path(target).parent}' && {'cp -r'} {source} {target}"

    # move_data(f"/{analysis.results_dir}",
    #     f"{EMG_CONFIG.slurm.ftp_results_dir}/{analysis.study.accession[:4]}/{analysis.study.accession}",
    #     'cp -r'
    # )

    move_data(source, target, move_command)
    # move_data(source, target, full_command)

