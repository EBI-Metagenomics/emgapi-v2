import os
import subprocess
from pathlib import Path

import pytest

from activate_django_first import EMG_CONFIG

import analyses.models as mg_models
from proteins import backfill
from proteins.backfill import Pending, pending, submit
from proteins.flows.backfill import proteindb_backfill
from proteins.tests.conftest import query

V = mg_models.Analysis.PipelineVersions
COMPLETED = mg_models.Analysis.AnalysisStates.ANALYSIS_COMPLETED
IMAGE = "docker://quay.io/microbiome-informatics/mgyp-accession:2026.3.01-abcdef0"


def analyse(assembly, study, version=V.v6, completed=True, results_dir="/results"):
    analysis = mg_models.Analysis.objects.create(
        study=study,
        sample=assembly.sample,
        assembly=assembly,
        ena_study=study.ena_study,
        pipeline_version=version,
        results_dir=results_dir,
    )
    analysis.mark_status(COMPLETED, completed)
    return analysis


def listed(analyses):
    return sorted((a.accession, a.pipeline_version, str(a.faa)) for a in analyses)


def faa(assembly, results_dir="/results"):
    accession = assembly.first_accession
    return f"{results_dir}/cds/{accession}_predicted_cds.faa.gz"


@pytest.mark.django_db(databases=["default", "proteindb"])
def test_pending_lists_the_newest_completed_v6_assembly_analysis_not_registered(
    tier1, mgnify_assemblies, raw_reads_mgnify_study
):
    a, b, c = mgnify_assemblies
    study = raw_reads_mgnify_study
    analyse(a, study, results_dir="/old")
    analyse(a, study, results_dir="/new")
    analyse(a, study, V.v5)
    analyse(b, study, V.v6_1)
    analyse(b, study, completed=False)
    analyse(b, study, V.v6_2, results_dir=None)
    analyse(c, study)
    analyse(c, study, V.v6_2)
    query(
        "INSERT INTO proteindb.assembly (accession, pipeline_version) VALUES (%s, '6.0')",
        [c.first_accession],
    )

    analyses = pending("cds", "qc")

    assert listed(analyses) == sorted(
        [
            (a.first_accession, "6.0", faa(a, "/new")),
            (b.first_accession, "6.1", faa(b)),
            (c.first_accession, "6.2", faa(c)),
        ]
    )
    (of_b,) = [x for x in analyses if x.accession == b.first_accession]
    assert of_b.gff == Path(f"/results/cds/{b.first_accession}_predicted_cds.gff.gz")
    assert of_b.contigs == Path(
        f"/results/qc/{b.first_accession}_filtered_contigs.fasta.gz"
    )


@pytest.mark.django_db(databases=["default", "proteindb"])
def test_pending_can_be_restricted_to_one_study(
    tier1, mgnify_assemblies, raw_reads_mgnify_study, webin_private_study
):
    a, b = mgnify_assemblies[:2]
    analyse(a, raw_reads_mgnify_study)
    analyse(b, webin_private_study)

    assert listed(pending("cds", "qc", webin_private_study.accession)) == [
        (b.first_accession, "6.0", faa(b))
    ]


def inputs(tmp_path, accession, present=True):
    analysis = Pending(
        accession,
        "6.0",
        *(tmp_path / accession / name for name in ("faa.gz", "gff.gz", "fasta.gz")),
    )
    if present:
        analysis.faa.parent.mkdir()
        for path in analysis[2:]:
            path.touch()
    return analysis


@pytest.fixture
def cluster(monkeypatch):
    """Records each command run, and pulls an empty image."""
    calls = []

    def run(argv, **kwargs):
        calls.append(argv)
        if argv[:2] == ["singularity", "pull"]:
            Path(argv[2]).touch()
            return subprocess.CompletedProcess(argv, 0, "", "")
        return subprocess.CompletedProcess(argv, 0, f"{100 + len(calls)};codon\n", "")

    monkeypatch.setattr(subprocess, "run", run)
    return calls


def test_submit_lists_the_analyses_and_chains_arrays_of_those_with_every_input(
    tmp_path, cluster
):
    root = tmp_path / "backfill"
    ready = [inputs(tmp_path, f"ERZ{n}") for n in range(3)]
    missing = inputs(tmp_path, "ERZ9", present=False)

    submitted = submit(root, [ready[0], missing, *ready[1:]], IMAGE, 8, 2)

    sif = root / "images" / "mgyp-accession_2026.3.01-abcdef0.sif"
    tasks = submitted.run / "tasks.tsv"
    pull, first, second = cluster
    assert pull == ["singularity", "pull", str(sif.with_suffix(".partial")), IMAGE]
    assert sif.exists()
    common = [
        "--job-name=proteindb-backfill",
        "--cpus-per-task=4",
        "--mem=16G",
        "--time=4:00:00",
        f"--output={submitted.run / 'logs' / '%A_%a.out'}",
    ]
    assert first == [
        "sbatch",
        "--parsable",
        "--array=1-2%8",
        *common,
        str(backfill.TASK),
        str(tasks),
        "0",
        str(sif),
    ]
    assert second == [
        "sbatch",
        "--parsable",
        "--array=1-1%8",
        *common,
        "--dependency=afterany:102",
        str(backfill.TASK),
        str(tasks),
        "2",
        str(sif),
    ]
    assert submitted.jobs == [102, 103]
    assert submitted.ready == 3
    assert submitted.missing == [missing]
    assert tasks.read_text().splitlines() == [
        "\t".join(
            map(
                str,
                [
                    *a,
                    root / a.accession / "6.0" / f"{a.accession}_predicted_cds.faa.gz",
                ],
            )
        )
        for a in ready
    ]
    assert (submitted.run / "missing.tsv").read_text() == "\t".join(
        map(str, missing)
    ) + "\n"


def test_submit_pulls_the_image_once(tmp_path, cluster):
    root = tmp_path / "backfill"
    analysis = inputs(tmp_path, "ERZ1")
    submit(root, [analysis], IMAGE, 8, 1000)
    submit(root, [analysis], IMAGE, 8, 1000)

    assert [argv[0] for argv in cluster] == ["singularity", "sbatch", "sbatch"]


def test_submit_with_nothing_ready_submits_nothing(tmp_path, cluster):
    submitted = submit(
        tmp_path / "backfill",
        [inputs(tmp_path, "ERZ1", present=False)],
        IMAGE,
        8,
        1000,
    )

    assert cluster == []
    assert submitted.jobs == []
    assert (submitted.run / "tasks.tsv").read_text() == ""


def test_a_task_accessions_its_line_in_the_image(tmp_path):
    bin = tmp_path / "bin"
    bin.mkdir()
    (bin / "singularity").write_text(
        f'#!/bin/bash\nprintf "%s\\n" "$@" > {tmp_path / "argv"}\n'
    )
    (bin / "singularity").chmod(0o755)
    rows = [
        [
            f"ERZ{n}",
            "6.1",
            f"/r{n}/cds/f",
            f"/r{n}/cds/g",
            f"/r{n}/qc/c",
            tmp_path / f"o{n}" / "out.faa.gz",
        ]
        for n in range(4)
    ]
    tasks = tmp_path / "tasks.tsv"
    backfill.write(tasks, rows)

    subprocess.run(
        [str(backfill.TASK), str(tasks), "2", "/images/m.sif"],
        env={
            **os.environ,
            "PATH": f"{bin}:{os.environ['PATH']}",
            "SLURM_ARRAY_TASK_ID": "1",
        },
        check=True,
    )

    out = tmp_path / "o2" / "out.faa.gz"
    assert out.parent.is_dir()
    assert (tmp_path / "argv").read_text().splitlines() == [
        "exec",
        "-B",
        "/r2/cds",
        "-B",
        "/r2/qc",
        "-B",
        str(out.parent),
        "/images/m.sif",
        "mgyp-accession",
        "--assembly",
        "ERZ2",
        "--pipeline-version",
        "6.1",
        "--faa",
        "/r2/cds/f",
        "--gff",
        "/r2/cds/g",
        "--contigs",
        "/r2/qc/c",
        "--out",
        str(out),
    ]


@pytest.fixture
def configured(monkeypatch, tmp_path):
    monkeypatch.setenv("PROTEINDB_DSN", "host=proteindb.invalid")
    monkeypatch.setattr(EMG_CONFIG.proteindb, "root", str(tmp_path))
    monkeypatch.setattr(EMG_CONFIG.proteindb, "accession_image", IMAGE)


@pytest.mark.django_db(databases=["default", "proteindb"], transaction=True)
def test_the_flow_submits_what_is_not_registered(
    prefect_harness,
    tier1,
    configured,
    cluster,
    tmp_path,
    mgnify_assemblies,
    raw_reads_mgnify_study,
):
    with_inputs, without = mgnify_assemblies[:2]
    for assembly in (with_inputs, without):
        analyse(assembly, raw_reads_mgnify_study, results_dir=str(tmp_path / "results"))
    accession = with_inputs.first_accession
    for path in (
        f"cds/{accession}_predicted_cds.faa.gz",
        f"cds/{accession}_predicted_cds.gff.gz",
        f"qc/{accession}_filtered_contigs.fasta.gz",
    ):
        (tmp_path / "results" / path).parent.mkdir(parents=True, exist_ok=True)
        (tmp_path / "results" / path).touch()

    submitted = proteindb_backfill()

    assert submitted.run.parent == tmp_path / "backfill" / "runs"
    assert submitted.ready == 1
    assert [a.accession for a in submitted.missing] == [without.first_accession]
    assert submitted.jobs == [102]


@pytest.mark.parametrize(
    "unset, message",
    [("PROTEINDB_DSN", "PROTEINDB_DSN"), ("accession_image", "ACCESSION_IMAGE")],
)
def test_the_flow_needs_the_connection_and_the_image(
    prefect_harness, configured, monkeypatch, cluster, unset, message
):
    if unset == "PROTEINDB_DSN":
        monkeypatch.delenv(unset)
    else:
        monkeypatch.setattr(EMG_CONFIG.proteindb, unset, "")

    with pytest.raises(RuntimeError, match=message):
        proteindb_backfill()
    assert cluster == []
