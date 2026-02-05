import pytest
import responses
import pathlib

from textwrap import dedent
import logging


from activate_django_first import EMG_CONFIG


from workflows.flows.analyse_study_tasks.shared.dwcr_generator import (
    generate_dwc_ready_summary_for_pipeline_run,
    merge_dwc_ready_summaries,
)
from analyses.models import Study


@pytest.mark.django_db(transaction=True)
@responses.activate
def test_dwcr_generator(
    amplicon_analysis_with_downloads,
    raw_reads_mgnify_study: Study,
    prefect_harness,
    completed_runs_filename: str = EMG_CONFIG.amplicon_pipeline.completed_runs_csv,
):
    amplicon_pipeline_outdir = pathlib.Path(
        amplicon_analysis_with_downloads.pipeline_outdir
    )
    refdb_otus = pathlib.Path(EMG_CONFIG.amplicon_pipeline.refdb_otus_dir)

    # Mock the read_run ENA API requests
    responses.add(
        responses.GET,
        EMG_CONFIG.ena.portal_search_api,
        json=[
            {
                "run_accession": "SRR1111111",
                "secondary_study_accession": "SRP2222222",
                "sample_accession": "SAMN3333333",
                "instrument_model": "Illumina MiSeq",
            }
        ],
        match=[
            responses.matchers.query_param_matcher(
                {
                    "result": "read_run",
                    "includeAccessions": "SRR1111111",
                    "fields": "secondary_study_accession,sample_accession,instrument_model",
                    "limit": "10",
                    "format": "json",
                    "download": "false",
                }
            )
        ],
    )

    responses.add(
        responses.GET,
        EMG_CONFIG.ena.portal_search_api,
        json=[
            {
                "run_accession": "SRR6180434",
                "secondary_study_accession": "SRP4444444",
                "sample_accession": "SAMN5555555",
                "instrument_model": "Illumina MiSeq",
            }
        ],
        match=[
            responses.matchers.query_param_matcher(
                {
                    "result": "read_run",
                    "includeAccessions": "SRR6180434",
                    "fields": "secondary_study_accession,sample_accession,instrument_model",
                    "limit": "10",
                    "format": "json",
                    "download": "false",
                }
            )
        ],
    )

    # mock the sample ENA API requests
    responses.add(
        responses.GET,
        EMG_CONFIG.ena.portal_search_api,
        json=[
            {
                "lat": 12,
                "lon": 22,
                "collection_date": "2025-05-25",
                "depth": 0.12,
                "center_name": "Devonshire Building",
                "temperature": 12,
                "salinity": 0.12,
                "country": "United Kingdom",
            }
        ],
        match=[
            responses.matchers.query_param_matcher(
                {
                    "result": "sample",
                    "includeAccessions": "SAMN3333333",
                    "fields": "lat,lon,collection_date,depth,center_name,temperature,salinity,country",
                    "limit": "10",
                    "format": "json",
                    "download": "false",
                }
            )
        ],
    )

    responses.add(
        responses.GET,
        EMG_CONFIG.ena.portal_search_api,
        json=[
            {
                "lat": 12,
                "lon": 22,
                "collection_date": "2025-05-25",
                "depth": 0.12,
                "center_name": "Devonshire Building",
                "temperature": 12,
                "salinity": 0.12,
                "country": "United Kingdom",
            }
        ],
        match=[
            responses.matchers.query_param_matcher(
                {
                    "result": "sample",
                    "includeAccessions": "SAMN5555555",
                    "fields": "lat,lon,collection_date,depth,center_name,temperature,salinity,country",
                    "limit": "10",
                    "format": "json",
                    "download": "false",
                }
            )
        ],
    )

    generate_dwc_ready_summary_for_pipeline_run(
        raw_reads_mgnify_study.accession,
        amplicon_pipeline_outdir,
        refdb_otus,
        completed_runs_filename,
    )


@pytest.mark.django_db(transaction=True)
def test_dwcr_merge(
    raw_reads_mgnify_study: Study,
    prefect_harness,
):
    prj_accession = "PRJNA398089"

    amplicon_folder = pathlib.Path(
        f"{EMG_CONFIG.slurm.default_workdir}/{prj_accession}/amplicon_v6"
    )
    amplicon_folder.mkdir(exist_ok=True, parents=True)
    logging.info(amplicon_folder)
    logging.info(amplicon_folder.exists())

    # create fake dwcr files for testing
    with open(amplicon_folder / "abc123_DADA2_SILVA_16S-V3-V4_dwcready.csv", "w") as fw:
        fw.write(
            dedent(
                """\
        ASVID,StudyID,SampleID,RunID,decimalLongitude,decimalLatitude,depth,temperature,salinity,collectionDate,seq_meth,country,InstitutionCode,amplifiedRegion,ASVCaller,ReferenceDatabase,TaxAnnotationTool,Superkingdom,Kingdom,Phylum,Class,Order,Family,Genus,Species,taxonID,MeasurementUnit,MeasurementValue,dbhit,dbhitIdentity,dbhitStart,dbhitEnd,ASVSeq
        seq_1,ERP122862,SAMEA7057179,ERR4334351,10.408889,51.112278,0.1,NA,NA,2018-05-02,Illumina MiSeq,Germany,FRIEDRICH SCHILLER UNIVERSITY JENA,16S-V3-V4,DADA2,SILVA,MAPseq,Bacteria,NA,Pseudomonadota,Alphaproteobacteria,Hyphomicrobiales,Nitrobacteraceae,Bradyrhizobium,NA,374,Number of reads,308,HE818674.1.1466,1.0,284,686,TGGGGAATATTGGACAATGGGCGCAAGCCTGATCCAGCCATGCCGCGTGAGTGATGAAGGCCCTAGGGTTGTAAAGCTCTTTTGTGCGGGAAGATAATGACGGTACCGCAAGAATAAGCCCCGGCTAACTTCGTGCCAGCAGCCGCGGTAATACGAAGGGGGCTAGCGTTGCTCGGAATCACTGGGCGTAAAGGGTGCGTAGGCGGGTCTTTAAGTCAGGGGTGAAATCCTGGAGCTCAACTCCAGAACTGCCTTTGATACTGAGGATCTTGAGTTCGGGAGAGGTGAGTGGAACTGCGAGTGTAGAGGTGAAATTCGTAGATATTCGCAAGAACACCAGTGGCGAAGGCGGCTCACTGGCCCGATACTGACGCTGAGGCACGAAAGCGTGGGGAGCAAACA
        seq_10,ERP122862,SAMEA7057179,ERR4334351,10.408889,51.112278,0.1,NA,NA,2018-05-02,Illumina MiSeq,Germany,FRIEDRICH SCHILLER UNIVERSITY JENA,16S-V3-V4,DADA2,SILVA,MAPseq,Bacteria,NA,Acidobacteriota,NA,NA,NA,NA,NA,57723,Number of reads,153,JN023865.1.1456,1.0,293,695,TGGGGAATTTTGCGCAATGGGGGAAACCCTGACGCAGCAACGCCGCGTGGAGGATGAAGCCCCTTGGGGTGTAAACTCCTTTCGACCGGGAAAATAATGATGGTACCGGTGGAAGAAGCACCGGCTAACTCTGTGCCAGCAGCCGCGGTAATACAGAGGGTGCGAGCGTTGTTCGGAATTATTGGGCGTAAAGGGCGCGTAGGCGGTGTTGTAAGTCACCTGTGAAACCTCTGGGCTTAACTCAGAGCCTGCAGGCGAAACTGCAATGCTGGAGGGTGGGAGAGGTGCGTGGAATTCCCGGTGTAGCGGTGAAATGCGTAGATATCGGGAGGAACACCTGTGGCGAAAGCGGCGCACTGGACCACTACTGACGCTGAGGCGCGAAAGCTAGGGGAGCAAACA
        seq_101,ERP122862,SAMEA7057179,ERR4334351,10.408889,51.112278,0.1,NA,NA,2018-05-02,Illumina MiSeq,Germany,FRIEDRICH SCHILLER UNIVERSITY JENA,16S-V3-V4,DADA2,SILVA,MAPseq,Bacteria,NA,Actinomycetota,Actinomycetes,Acidothermales,Acidothermaceae,Acidothermus,NA,28048,Number of reads,48,JF095799.1.1337,0.9901719689369202,324,731,TGGGGAATATTGCGCAATGGGCGGAAGCCTGACGCAGCGACGCCGCGTGCGGGATGAAGGCCTTCGGGTTGTAAACCGCTTTCACCAGGGACGAAGCGGAAGTGACGGTACCTGGGGAAGAAGCGCCGGCTAACTACGTGCCAGCAGCCGCGGTAATACGTAGGGCGCGAGCGTTGTCCGGAATTATTGGGCGTAAAGAGCTCGTAGGCGGCTGGTTGCGTCTGCTGTGAAATCCCGGGGCTTAACCCCGGGCGTGCAGTGGATACGGGCCGGCTGGAGGCAGGCAGGGGAGAATGGAATTCCCGGTGTAGCGGTGAAATGCGCAGATATCGGGAGGAACACCGGTGGCGAAGGCGGTTCTCTGGGCCTGTACTGACGCTGAGGAGCGAAAGCGTGGGGAGCAAACA
        seq_102,ERP122862,SAMEA7057179,ERR4334351,10.408889,51.112278,0.1,NA,NA,2018-05-02,Illumina MiSeq,Germany,FRIEDRICH SCHILLER UNIVERSITY JENA,16S-V3-V4,DADA2,SILVA,MAPseq,Bacteria,NA,Acidobacteriota,Terriglobia,Terriglobales,Acidobacteriaceae,NA,bacterium_Ellin327,204666,Number of reads,45,AF498709.1.1397,1.0,325,727,TGGGGAATTTTGCGCAATGGGGGAAACCCTGACGCAGCAACGCCGCGTGGAGGATGAAGTCCCTTGGGATGTAAACTCCTTTCGATCGGGACGATTATGACGGTACCGGAAGAAGAAGCACCGGCTAACTCTGTGCCAGCAGCCGCGGTAATACAGAGGGTGCAAGCGTTGTTCGGAATTATTGGGCGTAAAGGGTGCGTAGGCGGTGCGGTAAGTCTGTAGTGAAATCTCTGGGCTCAACTCAGAGTCTGCTATAGAAACTGCCGTGCTAGAGTGTGGGAGAGGTGAGTGGAATTCCCGGTGTAGCGGTGAAATGCGTAGATATCGGGAGGAACACCTGTGGCGAAAGCGGCTCACTGGACCACAACTGACGCTGATGCACGAAAGCTAGGGGAGCAAACA
        seq_103,ERP122862,SAMEA7057179,ERR4334351,10.408889,51.112278,0.1,NA,NA,2018-05-02,Illumina MiSeq,Germany,FRIEDRICH SCHILLER UNIVERSITY JENA,16S-V3-V4,DADA2,SILVA,MAPseq,Bacteria,NA,Acidobacteriota,NA,NA,NA,NA,NA,57723,Number of reads,45,GQ339135.1.1517,0.9953271150588988,360,788,TGAGGAATCTTGCGCAATGGGCGAAAGCCTGACGCAGCGACGCCGCGTGGAGGATGAAGGTCTTCGGATTGTAAACTCCTGTCGAGTGGGACGAATGCCGTGAGGGTGAACAATCCTTACGGTTGACGGTACCGCTGGAGGAAGCCCCGGCTAACTCCGTGCCAGCAGCCGCGGTAATACGGAGGGGGCTAGCGTTGTTCGGAATCATTGGGCGTAAAGGGCGCGTAGGCGGCCGATCAAGTCGGAGGTGAAATCCCTCGGCTCAACTGAGGAATTGCCTTCGATACTGTTCGGCTTGAGTCCTGGAGAGGGTAGCGGAATTCCCGGTGTAGCGGTGAAATGCGTAGAGACCGGGAAGAACACCAGTGGCGAAGGCGGCTACCTGGACAGGTACTGACGCTGAAGCGCGAAAGCTAGGGGAGCAAACA
                """
            )
        )

    with open(amplicon_folder / "def456_DADA2_SILVA_16S-V3-V4_dwcready.csv", "w") as fw:
        fw.write(
            dedent(
                """\
        ASVID,StudyID,SampleID,RunID,decimalLongitude,decimalLatitude,depth,temperature,salinity,collectionDate,seq_meth,country,InstitutionCode,amplifiedRegion,ASVCaller,ReferenceDatabase,TaxAnnotationTool,Superkingdom,Kingdom,Phylum,Class,Order,Family,Genus,Species,taxonID,MeasurementUnit,MeasurementValue,dbhit,dbhitIdentity,dbhitStart,dbhitEnd,ASVSeq
        seq_1,ERP122862,SAMEA7057179,ERR4334351,10.408889,51.112278,0.1,NA,NA,2018-05-02,Illumina MiSeq,Germany,FRIEDRICH SCHILLER UNIVERSITY JENA,16S-V3-V4,DADA2,SILVA,MAPseq,Bacteria,NA,Pseudomonadota,Alphaproteobacteria,Hyphomicrobiales,Nitrobacteraceae,Bradyrhizobium,NA,374,Number of reads,308,HE818674.1.1466,1.0,284,686,TGGGGAATATTGGACAATGGGCGCAAGCCTGATCCAGCCATGCCGCGTGAGTGATGAAGGCCCTAGGGTTGTAAAGCTCTTTTGTGCGGGAAGATAATGACGGTACCGCAAGAATAAGCCCCGGCTAACTTCGTGCCAGCAGCCGCGGTAATACGAAGGGGGCTAGCGTTGCTCGGAATCACTGGGCGTAAAGGGTGCGTAGGCGGGTCTTTAAGTCAGGGGTGAAATCCTGGAGCTCAACTCCAGAACTGCCTTTGATACTGAGGATCTTGAGTTCGGGAGAGGTGAGTGGAACTGCGAGTGTAGAGGTGAAATTCGTAGATATTCGCAAGAACACCAGTGGCGAAGGCGGCTCACTGGCCCGATACTGACGCTGAGGCACGAAAGCGTGGGGAGCAAACA
        seq_10,ERP122862,SAMEA7057179,ERR4334351,10.408889,51.112278,0.1,NA,NA,2018-05-02,Illumina MiSeq,Germany,FRIEDRICH SCHILLER UNIVERSITY JENA,16S-V3-V4,DADA2,SILVA,MAPseq,Bacteria,NA,Acidobacteriota,NA,NA,NA,NA,NA,57723,Number of reads,153,JN023865.1.1456,1.0,293,695,TGGGGAATTTTGCGCAATGGGGGAAACCCTGACGCAGCAACGCCGCGTGGAGGATGAAGCCCCTTGGGGTGTAAACTCCTTTCGACCGGGAAAATAATGATGGTACCGGTGGAAGAAGCACCGGCTAACTCTGTGCCAGCAGCCGCGGTAATACAGAGGGTGCGAGCGTTGTTCGGAATTATTGGGCGTAAAGGGCGCGTAGGCGGTGTTGTAAGTCACCTGTGAAACCTCTGGGCTTAACTCAGAGCCTGCAGGCGAAACTGCAATGCTGGAGGGTGGGAGAGGTGCGTGGAATTCCCGGTGTAGCGGTGAAATGCGTAGATATCGGGAGGAACACCTGTGGCGAAAGCGGCGCACTGGACCACTACTGACGCTGAGGCGCGAAAGCTAGGGGAGCAAACA
        seq_101,ERP122862,SAMEA7057179,ERR4334351,10.408889,51.112278,0.1,NA,NA,2018-05-02,Illumina MiSeq,Germany,FRIEDRICH SCHILLER UNIVERSITY JENA,16S-V3-V4,DADA2,SILVA,MAPseq,Bacteria,NA,Actinomycetota,Actinomycetes,Acidothermales,Acidothermaceae,Acidothermus,NA,28048,Number of reads,48,JF095799.1.1337,0.9901719689369202,324,731,TGGGGAATATTGCGCAATGGGCGGAAGCCTGACGCAGCGACGCCGCGTGCGGGATGAAGGCCTTCGGGTTGTAAACCGCTTTCACCAGGGACGAAGCGGAAGTGACGGTACCTGGGGAAGAAGCGCCGGCTAACTACGTGCCAGCAGCCGCGGTAATACGTAGGGCGCGAGCGTTGTCCGGAATTATTGGGCGTAAAGAGCTCGTAGGCGGCTGGTTGCGTCTGCTGTGAAATCCCGGGGCTTAACCCCGGGCGTGCAGTGGATACGGGCCGGCTGGAGGCAGGCAGGGGAGAATGGAATTCCCGGTGTAGCGGTGAAATGCGCAGATATCGGGAGGAACACCGGTGGCGAAGGCGGTTCTCTGGGCCTGTACTGACGCTGAGGAGCGAAAGCGTGGGGAGCAAACA
        seq_102,ERP122862,SAMEA7057179,ERR4334351,10.408889,51.112278,0.1,NA,NA,2018-05-02,Illumina MiSeq,Germany,FRIEDRICH SCHILLER UNIVERSITY JENA,16S-V3-V4,DADA2,SILVA,MAPseq,Bacteria,NA,Acidobacteriota,Terriglobia,Terriglobales,Acidobacteriaceae,NA,bacterium_Ellin327,204666,Number of reads,45,AF498709.1.1397,1.0,325,727,TGGGGAATTTTGCGCAATGGGGGAAACCCTGACGCAGCAACGCCGCGTGGAGGATGAAGTCCCTTGGGATGTAAACTCCTTTCGATCGGGACGATTATGACGGTACCGGAAGAAGAAGCACCGGCTAACTCTGTGCCAGCAGCCGCGGTAATACAGAGGGTGCAAGCGTTGTTCGGAATTATTGGGCGTAAAGGGTGCGTAGGCGGTGCGGTAAGTCTGTAGTGAAATCTCTGGGCTCAACTCAGAGTCTGCTATAGAAACTGCCGTGCTAGAGTGTGGGAGAGGTGAGTGGAATTCCCGGTGTAGCGGTGAAATGCGTAGATATCGGGAGGAACACCTGTGGCGAAAGCGGCTCACTGGACCACAACTGACGCTGATGCACGAAAGCTAGGGGAGCAAACA
        seq_103,ERP122862,SAMEA7057179,ERR4334351,10.408889,51.112278,0.1,NA,NA,2018-05-02,Illumina MiSeq,Germany,FRIEDRICH SCHILLER UNIVERSITY JENA,16S-V3-V4,DADA2,SILVA,MAPseq,Bacteria,NA,Acidobacteriota,NA,NA,NA,NA,NA,57723,Number of reads,45,GQ339135.1.1517,0.9953271150588988,360,788,TGAGGAATCTTGCGCAATGGGCGAAAGCCTGACGCAGCGACGCCGCGTGGAGGATGAAGGTCTTCGGATTGTAAACTCCTGTCGAGTGGGACGAATGCCGTGAGGGTGAACAATCCTTACGGTTGACGGTACCGCTGGAGGAAGCCCCGGCTAACTCCGTGCCAGCAGCCGCGGTAATACGGAGGGGGCTAGCGTTGTTCGGAATCATTGGGCGTAAAGGGCGCGTAGGCGGCCGATCAAGTCGGAGGTGAAATCCCTCGGCTCAACTGAGGAATTGCCTTCGATACTGTTCGGCTTGAGTCCTGGAGAGGGTAGCGGAATTCCCGGTGTAGCGGTGAAATGCGTAGAGACCGGGAAGAACACCAGTGGCGAAGGCGGCTACCTGGACAGGTACTGACGCTGAAGCGCGAAAGCTAGGGGAGCAAACA
                """
            )
        )

    merge_dwc_ready_summaries(raw_reads_mgnify_study.accession, False, True)
