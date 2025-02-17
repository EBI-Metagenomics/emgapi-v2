import re
from pathlib import Path

from prefect import task, get_run_logger

from activate_django_first import EMG_CONFIG

import analyses.models
from workflows.flows.analyse_study_tasks.analysis_states import AnalysisStates
from workflows.prefect_utils.analyses_models_helpers import task_mark_analysis_status
from workflows.prefect_utils.cache_control import context_agnostic_task_input_hash


@task(
    cache_key_fn=context_agnostic_task_input_hash,
)
def sanity_check_amplicon_results(
    amplicon_current_outdir: Path, analysis: analyses.models.Analysis
):
    """
    QC folder:
        required:
         - ${run_id}_seqfu.tsv
        optional:
         - ${run_id}.merged.fastq.gz / ${run_id}.fastp.fastq.gz
         - ${run_id}.fastp.json
         - ${run_id}_suffix_header_err.json
         - ${run_id}_multiqc_report.html
    SEQUENCE CATEGORISATION folder:
        required:
         - ${run_id}_${gene}.fasta (depending on if the gene was SSU/LSU/ITS)
         - ${run_id}.tblout.deoverlapped
         - ${run_id}_${gene}_rRNA_${domain}.${domain_id}.fa (domain can be bacteria/archaea/eukarya)
    AMPLIFIED REGION INFERENCE folder:
        required:
         - ${run_id}.tsv
        optional:
         - ${run_id}.*S.${V?}.txt - max 2 files, if passed inference thresholds, example, ERR4334351.16S.V3-V4.txt
    PRIMER IDENTIFICATION folder:
        if only required file present - it should be empty
        if 3 files are present they can be all not empty or all empty
        required:
         - ${run_id}.cutadapt.json - if ony that file it should be empty
        optional (if ${run_id}.cutadapt.json not empty):
         - ${run_id}_primers.fasta
         - ${run_id}_primer_validation.tsv
    ASV:
        required:
         - ${run_id}_dada2_stats.tsv
         - ${run_id}_DADA2-SILVA_asv_tax.tsv
         - ${run_id}_DADA2-PR2_asv_tax.tsv
         - ${run_id}_asv_seqs.fasta
         - /${var_region}
         - /${var_region}/${run_id}_${var_region}_asv_read_counts.tsv
        optional:
         - second var region
         - concat (for both var regions)
    TAXONOMY SUMMARY:
        optional:
         - SILVA-SSU
         - PR2
         - UNITE
         - ITSoneDB
            - {run_id}.html
            - ${run_id}_{db_label}.mseq
            - ${run_id}_{db_label}.tsv
            - ${run_id}_${db_label}.txt
         - DADA2-SILVA
         - DADA2-PR2
            - ${run_id}_${db_label}.mseq
            for 1 var region:
             - ${run_id}_${var_region}_{db_label}_asv_krona_counts.txt
             - ${run_id}_${var_region}.html
            for 2 var regions:
             - ${run_id}_${var_region1}_{db_label}_asv_krona_counts.txt
             - ${run_id}_${var_region1}.html
             - ${run_id}_${var_region2}_{db_label}_asv_krona_counts.txt
             - ${run_id}_${var_region2}.html
             - ${run_id}_concat_{db_label}_asv_krona_counts.txt
             - ${run_id}_concat.html
    """
    logger = get_run_logger()
    reason = None
    run_id = analysis.run.first_accession
    qc_folder = Path(
        f"{amplicon_current_outdir}/{EMG_CONFIG.amplicon_pipeline.qc_folder}"
    )
    sequence_categorisation_folder = Path(
        f"{amplicon_current_outdir}/{EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder}"
    )
    amplified_region_inference_folder = Path(
        f"{amplicon_current_outdir}/{EMG_CONFIG.amplicon_pipeline.amplified_region_inference_folder}"
    )
    primer_identification_folder = Path(
        f"{amplicon_current_outdir}/{EMG_CONFIG.amplicon_pipeline.primer_identification_folder}"
    )
    asv_folder = Path(
        f"{amplicon_current_outdir}/{EMG_CONFIG.amplicon_pipeline.asv_folder}"
    )
    taxonomy_summary_folder = Path(
        f"{amplicon_current_outdir}/{EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}"
    )

    # SEQUENCE CATEGORISATION optional folder
    if sequence_categorisation_folder.exists():
        # if folder exists - checking for required files
        pattern_gene_fasta = re.compile(r"\w+_(SSU|LSU|ITS)\.fasta$")
        matching_gene_files = [
            True if f.is_file() and pattern_gene_fasta.match(f.name) else False
            for f in sequence_categorisation_folder.iterdir()
        ]
        pattern_domain_fasta = re.compile(
            r"\w+_(SSU|LSU|ITS)_rRNA_(bacteria|archaea|eukarya)\.[A-Z0-9]+\.fa$"
        )
        matching_domain_files = [
            True if f.is_file() and pattern_domain_fasta.match(f.name) else False
            for f in sequence_categorisation_folder.iterdir()
        ]
        if not (
            Path(
                f"{sequence_categorisation_folder}/{run_id}.tblout.deoverlapped"
            ).exists()
            and sum(matching_gene_files)
            and sum(matching_domain_files)
        ):
            reason = f"missing required files in {EMG_CONFIG.amplicon_pipeline.sequence_categorisation_folder}"
            logger.info(f"Post sanity check for {run_id}: {reason}")

    amplified_regions = []
    # AMPLIFIED REGION INFERENCE optional folder
    if amplified_region_inference_folder.exists():
        if not Path(f"{amplified_region_inference_folder}/{run_id}.tsv").exists():
            reason = f"missing required file in {EMG_CONFIG.amplicon_pipeline.amplified_region_inference_folder}"
            logger.info(f"Post sanity check for {run_id}: {reason}")
        else:
            if len(list(amplified_region_inference_folder.iterdir())) > 1:
                # extract variable regions
                pattern_txt = re.compile(r"\w+\.(\w+S)\.(V[\w.-]+)\.txt$")
                for f in amplified_region_inference_folder.iterdir():
                    match = pattern_txt.search(f.name)
                    if match:
                        amplified_regions.append(f"{match.group(1)}-{match.group(2)}")
                if len(amplified_regions) > 2:
                    reason = "More than 2 variable regions were found"
                    logger.info(f"Post sanity check for {run_id}: {reason}")

    # PRIMER IDENTIFICATION optional folder
    if primer_identification_folder.exists():
        cutadapt_json = Path(f"{primer_identification_folder}/{run_id}.cutadapt.json")
        if len(list(primer_identification_folder.iterdir())) == 1:
            if not cutadapt_json.exists():
                # checking required file
                reason = f"missing required file in {EMG_CONFIG.amplicon_pipeline.primer_identification_folder}"
                logger.info(f"Post sanity check for {run_id}: {reason}")
            else:
                # checking it should be empty
                if cutadapt_json.stat().st_size:
                    reason = f"required file in {EMG_CONFIG.amplicon_pipeline.primer_identification_folder} did not passed sanity check"
                    logger.info(f"Post sanity check for {run_id}: {reason}")
        elif len(list(primer_identification_folder.iterdir())) == 3:
            primers_file = Path(
                f"{primer_identification_folder}/{run_id}_primers.fasta"
            )
            validation_file = Path(
                f"{primer_identification_folder}/{run_id}_primer_validation.tsv"
            )
            if (
                primers_file.exists()
                and validation_file.exists()
                and cutadapt_json.exists()
            ):
                if not (
                    (
                        primers_file.stat().st_size == 0
                        and validation_file.stat().st_size == 0
                        and cutadapt_json.stat().st_size == 0
                    )
                    or (
                        primers_file.stat().st_size != 0
                        and validation_file.stat().st_size != 0
                        and cutadapt_json.stat().st_size != 0
                    )
                ):
                    reason = f"Incorrect file sizes in {EMG_CONFIG.amplicon_pipeline.primer_identification_folder}"
                    logger.info(f"Post sanity check for {run_id}: {reason}")
            else:
                reason = f"Incorrect structure of {EMG_CONFIG.amplicon_pipeline.primer_identification_folder}"
                logger.info(f"Post sanity check for {run_id}: {reason}")
        else:
            reason = f"Incorrect number of files in {EMG_CONFIG.amplicon_pipeline.primer_identification_folder}"
            logger.info(f"Post sanity check for {run_id}: {reason}")

    # ASV optional folder
    if asv_folder.exists():
        dada2_stats = Path(f"{asv_folder}/{run_id}_dada2_stats.tsv")
        dada2_silva = Path(f"{asv_folder}/{run_id}_DADA2-SILVA_asv_tax.tsv")
        dada2_pr2 = Path(f"{asv_folder}/{run_id}_DADA2-PR2_asv_tax.tsv")
        asv_stats = Path(f"{asv_folder}/{run_id}_asv_seqs.fasta")
        if not (
            dada2_stats.exists()
            and dada2_pr2.exists()
            and dada2_silva.exists()
            and asv_stats.exists()
        ):
            reason = (
                f"missing required file in {EMG_CONFIG.amplicon_pipeline.asv_folder}"
            )
            logger.info(f"Post sanity check for {run_id}: {reason}")
        else:
            # check var regions
            if amplified_regions:
                for region in amplified_regions:
                    if Path(f"{asv_folder}/{region}").exists():
                        if not Path(
                            f"{asv_folder}/{region}/{run_id}_{region}_asv_read_counts.tsv"
                        ).exists():
                            reason = f"No asv_read_counts in {region}"
                    else:
                        reason = (
                            f"No {region} in {EMG_CONFIG.amplicon_pipeline.asv_folder}"
                        )
            # check concat folder for more than 1 region
            if len(amplified_regions) > 1:
                if Path(f"{asv_folder}/concat").exists():
                    if not Path(
                        f"{asv_folder}/concat/{run_id}_concat_asv_read_counts.tsv"
                    ).exists():
                        reason = f"No counts for concat folder in {EMG_CONFIG.amplicon_pipeline.asv_folder}"
                else:
                    reason = f"Missing concat folder in {EMG_CONFIG.amplicon_pipeline.asv_folder} for {len(amplified_regions)} regions"

    # TAXONOMY SUMMARY folder:
    if taxonomy_summary_folder.exists():
        dada2_tax_names = ["DADA2-SILVA", "DADA2-PR2"]
        tax_dbs = ["SILVA-SSU", "SILVA-LSU", "UNITE", "ITSoneDB", "PR2"]
        if asv_folder.exists():
            if (
                sum(
                    [
                        Path(f"{taxonomy_summary_folder}/{db}").exists()
                        for db in dada2_tax_names
                    ]
                )
                != 2
            ):
                reason = f"missing one of DADA2 tax folders in {EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}"
        else:
            if (
                sum(
                    [
                        Path(f"{taxonomy_summary_folder}/{db}").exists()
                        for db in dada2_tax_names
                    ]
                )
                != 0
            ):
                reason = f"DADA2 db exists but no {EMG_CONFIG.amplicon_pipeline.asv_folder} found"
        for db in taxonomy_summary_folder.iterdir():
            if db.name in tax_dbs:
                html = Path(f"{db}/{run_id}.html")
                mseq = Path(f"{db}/{run_id}_{db.name}.mseq")
                tsv = Path(f"{db}/{run_id}_{db.name}.tsv")
                txt = Path(f"{db}/{run_id}_{db.name}.txt")
                if not (
                    html.exists() and mseq.exists() and tsv.exists() and txt.exists()
                ):
                    reason = f"missing file in {db}"
            elif db.name in dada2_tax_names and asv_folder.exists():
                if not Path(f"{db}/{run_id}_{db.name}.mseq").exists():
                    reason = f"missing mseq in {db}"
                else:
                    for region in amplified_regions:
                        region_krona = Path(
                            f"{db}/{run_id}_{region}_{db.name}_asv_krona_counts.txt"
                        )
                        region_html = Path(f"{db}/{run_id}_{region}.html")
                        if not (region_html.exists() and region_krona.exists()):
                            reason = f"missing {region} file in {db}"
                    # checking concat folder
                    if len(amplified_regions) == 2:
                        concat_html = Path(f"{db}/{run_id}_concat.html")
                        concat_krona = Path(
                            f"{db}/{run_id}_concat_{db.name}_asv_krona_counts.txt"
                        )
                        if not (concat_krona.exists() and concat_html.exists()):
                            reason = f"missing concat files in {db}"
            else:
                reason = f"unknown {db} in {EMG_CONFIG.amplicon_pipeline.taxonomy_summary_folder}"

    # QC mandatory folder
    if qc_folder.exists():
        if not Path(f"{qc_folder}/{analysis.run.first_accession}_seqfu.tsv").exists():
            reason = f"No required seqfu.tsv in {EMG_CONFIG.amplicon_pipeline.qc_folder} folder"
    else:
        reason = f"No {EMG_CONFIG.amplicon_pipeline.qc_folder} folder"
    logger.info(f"Post sanity check for {run_id}: {reason}")

    if reason:
        task_mark_analysis_status(
            analysis,
            status=AnalysisStates.ANALYSIS_POST_SANITY_CHECK_FAILED,
            reason=reason,
        )
