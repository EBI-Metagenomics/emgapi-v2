import pytest
from django.core.management import call_command

from analyses.base_models.with_downloads_models import (
    DownloadFile,
    DownloadFileType,
    DownloadType,
)
from analyses.models import Analysis

WORKDIR_PATH = (
    "/nfs/production/rdf/metagenomics/projects/automation/slurm_workdir/"
    "PRJEB1/amplicon_v6.2/abc123/ERR1/asv/ERR1_asv_seqs.fasta"
)


def _analysis(study, sample, accession: str, asv_seqs_path: str) -> Analysis:
    analysis = Analysis.objects.create(
        accession=accession,
        study=study,
        sample=sample,
        ena_study=study.ena_study,
        experiment_type=Analysis.ExperimentTypes.AMPLICON,
    )
    for path, alias, group in (
        (
            "asv/ERR1_DADA2-SILVA_asv_tax.tsv",
            "ERR1_DADA2-SILVA_asv_tax.tsv",
            "asv.distribution",
        ),
        (asv_seqs_path, "ERR1_asv_seqs.fasta", "asv.sequences"),
    ):
        analysis.add_download(
            DownloadFile(
                path=path,
                alias=alias,
                download_group=group,
                file_type=DownloadFileType.TSV,
                download_type=DownloadType.TAXONOMIC_ANALYSIS,
                short_description="",
                long_description="",
            )
        )
    analysis.save()
    return analysis


def _paths(analysis: Analysis) -> dict[str, str]:
    analysis.refresh_from_db()
    return {d["download_group"]: d["path"] for d in analysis.downloads}


@pytest.mark.django_db
def test_absolute_asv_sequences_paths_are_made_relative_to_the_results_dir(
    raw_reads_mgnify_study, raw_reads_mgnify_sample
):
    sample = raw_reads_mgnify_sample[0]
    # As import_asv stored it before the fix, which add_download now refuses.
    broken = _analysis(
        raw_reads_mgnify_study, sample, "MGYA00000901", "asv/ERR1_asv_seqs.fasta"
    )
    broken.downloads[1]["path"] = WORKDIR_PATH
    broken.save()
    fine = _analysis(
        raw_reads_mgnify_study, sample, "MGYA00000902", "asv/ERR1_asv_seqs.fasta"
    )

    call_command("repair_asv_sequences_download_paths", "--dry-run")
    assert _paths(broken)["asv.sequences"] == WORKDIR_PATH

    call_command("repair_asv_sequences_download_paths")
    assert _paths(broken) == {
        "asv.distribution": "asv/ERR1_DADA2-SILVA_asv_tax.tsv",
        "asv.sequences": "asv/ERR1_asv_seqs.fasta",
    }
    assert _paths(fine)["asv.sequences"] == "asv/ERR1_asv_seqs.fasta"


@pytest.mark.django_db
def test_an_absolute_download_path_is_refused(
    raw_reads_mgnify_study, raw_reads_mgnify_sample
):
    with pytest.raises(ValueError, match="must be relative to the results dir"):
        _analysis(
            raw_reads_mgnify_study,
            raw_reads_mgnify_sample[0],
            "MGYA00000903",
            WORKDIR_PATH,
        )
