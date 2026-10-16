import re
import shutil
from datetime import timedelta
from pathlib import Path

from django.db import transaction
from django.utils import timezone

from activate_django_first import EMG_CONFIG

from genomes.models import GenomeCatalogue, GenomeSearchIndex
from workflows.prefect_utils.build_cli_command import cli_command
from workflows.prefect_utils.flows_utils import django_db_flow as flow
from workflows.prefect_utils.flows_utils import django_db_task as task
from workflows.prefect_utils.slurm_flow import run_cluster_job
from workflows.prefect_utils.slurm_policies import ResubmitAlwaysPolicy


@task
def make_lexicmap_index(catalogue_slug: str, results_directory: str):
    """Build on HPC, copy to shared storage, then activate the registry entry."""
    config = EMG_CONFIG.lexicmap
    catalogue = GenomeCatalogue.objects.get(pk=catalogue_slug)
    source = Path(results_directory) / "website"
    files = sorted(
        p.resolve()
        for p in source.glob("*/genome/MGYG*.fna*")
        if re.fullmatch(r"MGYG\d+\.fna(?:\.gz)?", p.name)
    )
    accessions = [p.name.split(".")[0] for p in files]
    expected = set(catalogue.genomes.values_list("genome__accession", flat=True))
    if not files or len(files) != len(expected) or set(accessions) != expected:
        raise ValueError("LexicMap FASTA inputs must match catalogue membership")

    index = GenomeSearchIndex(
        catalogue=catalogue, backend=GenomeSearchIndex.Backend.LEXICMAP
    )
    # Each build has its own path; searches keep using the old index until activation.
    index.artifact_path = f"{catalogue.pk}/{index.pk}.lmi"
    index.manifest_path = f"{index.artifact_path}/inputs.txt"
    index.save()
    workdir = Path(config.build_root) / str(index.pk)
    build = workdir / "index.lmi"
    published = Path(config.publish_root) / index.artifact_path
    try:
        workdir.mkdir(parents=True)
        manifest = workdir / "inputs.txt"
        manifest.write_text("".join(f"{path}\n" for path in files))
        skipped = workdir / "skipped.tsv"
        run_cluster_job(
            name=f"Build LexicMap index for {catalogue.pk}",
            command=cli_command(
                [
                    config.binary_path,
                    "index",
                    ("-X", manifest),
                    ("-O", build),
                    ("-j", config.threads),
                    ("-c", config.chunks),
                    ("-b", config.batch_size),
                    ("-G", skipped),
                    ("-g", 268435455),  # LexicMap's size limit.
                ]
            ),
            expected_time=timedelta(hours=24),
            memory=f"{config.memory_gb}G",
            cpus_per_task=config.threads,
            environment="ALL",
            working_dir=workdir,
            resubmit_policy=ResubmitAlwaysPolicy,
        )
        if not (build / "info.toml").is_file() or (
            skipped.exists() and skipped.read_text().strip()
        ):
            raise ValueError(f"Incomplete LexicMap index; inspect {workdir}")
        shutil.copyfile(manifest, build / "inputs.txt")
        run_cluster_job(
            name=f"Publish LexicMap index for {catalogue.pk}",
            command=cli_command(["mkdir", "-p", published])
            + " && "
            + cli_command(["rsync", "-a", f"{build}/", f"{published}/"]),
            expected_time=timedelta(hours=2),
            memory="1G",
            environment={},
            partition=EMG_CONFIG.slurm.datamover_partition,
            resubmit_policy=ResubmitAlwaysPolicy,
        )
        with transaction.atomic():
            GenomeCatalogue.objects.select_for_update().get(pk=catalogue.pk)
            GenomeSearchIndex.objects.filter(
                catalogue=catalogue,
                backend=index.backend,
                is_active=True,
            ).update(is_active=False, status=GenomeSearchIndex.Status.RETIRED)
            index.status = GenomeSearchIndex.Status.ACTIVE
            index.is_active = True
            index.genome_count = len(files)
            index.built_at = index.activated_at = timezone.now()
            index.save()
    except Exception:
        GenomeSearchIndex.objects.filter(pk=index.pk).update(
            status=GenomeSearchIndex.Status.FAILED
        )
        raise
    return str(index.pk)


@flow(name="Build catalogue LexicMap index")
def build_catalogue_lexicmap_index(catalogue_slug: str, results_directory: str):
    """Build search for an existing release without re-importing it."""
    return make_lexicmap_index(
        catalogue_slug=catalogue_slug, results_directory=results_directory
    )
