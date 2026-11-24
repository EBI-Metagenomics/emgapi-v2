"""Models of the MGYP database. sql/mgyp_schema.sql creates the tables, not Django migrations."""

from collections import defaultdict
from typing import Iterable

from django.db import connections, models
from django.db.models.functions import Now

from analyses.models import V6_GENE_CALLERS, GeneCallers
from proteins.accessioning.hashing import protein_hash


class UnsupportedPipelineVersion(Exception):
    """The protein ingestion does not know the gene callers of this pipeline version."""


class MGnifyProteinsBase(models.Model):
    """A table in the MGYP database. The router sends it to the mgyp alias."""

    class Meta:
        abstract = True
        managed = False


# Hashes in one lookup query. Each query reads one partition, so this only bounds the query's size.
LOOKUP_CHUNK = 5000


def mgyp(id: int) -> str:
    """The MGYP accession of a protein id.

    :param id: The protein's id.
    """
    return f"MGYP{id:012d}"


def partition(hash: bytes) -> int:
    """The protein table's partition of a hash: its top 6 bits, as in mgyp_schema.sql."""
    return hash[0] >> 2


class ProteinQuerySet(models.QuerySet):
    def ids_of_hashes(self, hashes: Iterable[bytes]) -> dict[bytes, int]:
        """The protein id of each hash that has one. Hashes without an id are not in the result.

        Each query has the hashes of one partition only. Django sends them as literals,
        so PostgreSQL reads only that partition.

        :param hashes: Protein hashes.
        """
        by_partition = defaultdict(list)
        for hash in hashes:
            by_partition[partition(hash)].append(hash)
        found = {}
        for group in by_partition.values():
            for start in range(0, len(group), LOOKUP_CHUNK):
                chunk = group[start : start + LOOKUP_CHUNK]
                found |= {
                    bytes(hash): id
                    for hash, id in self.filter(hash__in=chunk).values_list(
                        "hash", "protein_id"
                    )
                }
        return found

    def find_mgyps(self, sequences: Iterable[str]) -> dict[str, str]:
        """The MGYP of each sequence that has one. Sequences without an MGYP are not in the result.

        :param sequences: Protein sequences. Each must pass the hash contract.
        :return: Each sequence with its MGYP, for example {"MKV…": "MGYP000000000042"}.
        :raises InvalidSequence: When a sequence fails the hash contract.
        """
        by_hash = {Protein.hash_of(sequence): sequence for sequence in sequences}
        return {
            by_hash[hash]: mgyp(id) for hash, id in self.ids_of_hashes(by_hash).items()
        }

    def allocate(
        self, proteins: list[tuple[bytes, str]], assembly_proteins_id: int
    ) -> dict[bytes, int]:
        """Gives ids to new proteins, and stages the ones that this transaction inserted.

        Raw SQL, for correctness: RETURNING gives only the proteins that this INSERT added.
        The ORM cannot tell them from the proteins that another job added since the lookup,
        and a protein staged twice goes into the protein dataset twice.
        Run it in a transaction: the temporary table is dropped at the commit.

        :param proteins: (hash, sequence) of the proteins that the lookup did not find.
        :param assembly_proteins_id: The record that introduces them.
        :return: The id of each of the proteins, including those another job added since the lookup.
        """
        with connections[self.db].cursor() as cursor:
            cursor.execute(
                "CREATE TEMP TABLE new_protein (sequence text, hash bytea PRIMARY KEY) ON COMMIT DROP"
            )
            with cursor.copy(
                "COPY new_protein (hash, sequence) FROM STDIN (FORMAT binary)"
            ) as copy:
                copy.set_types(["bytea", "text"])
                for row in proteins:
                    copy.write_row(row)
            # In hash order, so that jobs with overlapping new proteins lock them in the same order.
            cursor.execute(
                """
                WITH ins AS (
                    INSERT INTO protein (hash, id)
                    SELECT hash, nextval('protein_id_seq') FROM new_protein ORDER BY hash
                    ON CONFLICT (hash) DO NOTHING
                    RETURNING hash, id
                )
                INSERT INTO staged_protein (id, hash, sequence, assembly_proteins_id)
                SELECT ins.id, ins.hash, n.sequence, %s FROM ins JOIN new_protein n USING (hash)
                """,
                [assembly_proteins_id],
            )
        return self.ids_of_hashes(hash for hash, _ in proteins)


class Protein(MGnifyProteinsBase):
    """A protein with an MGYP. The table has billions of rows: always filter by hash.

    Do not register it in the admin (the admin counts all rows), and do not write with it:
    only the accessioning core inserts proteins.
    """

    # The primary key is the hash. Django allows the field name "id" only on a primary key,
    # so the protein id has another name.
    hash = models.BinaryField(
        primary_key=True
    )  # sha256 of the sequence, hash contract v1
    # No unique constraint: a partitioned table cannot have one on a column outside the partition key.
    protein_id = models.BigIntegerField(db_column="id")

    objects = ProteinQuerySet.as_manager()

    class Meta(MGnifyProteinsBase.Meta):
        db_table = "protein"

    def __str__(self):
        return self.mgyp

    @property
    def mgyp(self) -> str:
        """The MGYP accession, for example MGYP000000000042."""
        return mgyp(self.protein_id)

    @staticmethod
    def hash_of(sequence: str) -> bytes:
        """The hash of a protein sequence, by the hash contract.

        :param sequence: A protein sequence.
        :return: 32 bytes.
        :raises InvalidSequence: When the sequence fails the hash contract.
        """
        return protein_hash(sequence)


class Assembly(MGnifyProteinsBase):
    """An assembly, by its ERZ. Its contigs and occurrences belong to it in the protein dataset."""

    accession = models.TextField(unique=True)  # ERZ

    class Meta(MGnifyProteinsBase.Meta):
        db_table = "assembly"

    def __str__(self):
        return self.accession


class AssemblyProteins(MGnifyProteinsBase):
    """The proteins of one assembly analysis, predicted by one version of the source pipeline."""

    class Source(models.TextChoices):
        """The pipeline that predicted the genes."""

        ASSEMBLY_ANALYSIS = "assembly_analysis", "Assembly analysis"
        GENE_CALLING = "gene_calling", "Gene calling + assembly analysis"

    assembly = models.ForeignKey(Assembly, on_delete=models.DO_NOTHING)
    source = models.TextField(choices=Source)
    # major.minor of the source pipeline, for example "6.0" (ASA) or "1.0" (gene calling)
    pipeline_version = models.TextField()
    # The assembly analysis (MGYA). With source gene_calling, the analysis that took the proteins.
    analysis_accession = models.TextField(unique=True)
    ingested_at = models.DateTimeField(db_default=Now())

    class Meta(MGnifyProteinsBase.Meta):
        db_table = "assembly_proteins"
        verbose_name_plural = "assembly proteins"
        constraints = [
            models.UniqueConstraint(
                fields=["assembly", "source", "pipeline_version"],
                name="assembly_proteins_assembly_source_version",
            )
        ]

    def __str__(self):
        return f"{self.assembly} {self.source} {self.pipeline_version} ({self.analysis_accession})"


class GeneCaller(MGnifyProteinsBase):
    """A gene caller and its version, for example Pyrodigal 3.6.3."""

    # Each gene caller of each source and pipeline version (major.minor), and whether its FASTA headers carry a partial= flag.
    # TODO (team): older analysis pipeline versions, for example "5.0".
    BY_PIPELINE_VERSION: dict[tuple[str, str], GeneCallers] = {
        (AssemblyProteins.Source.ASSEMBLY_ANALYSIS, "6.0"): V6_GENE_CALLERS,
        (AssemblyProteins.Source.ASSEMBLY_ANALYSIS, "6.1"): V6_GENE_CALLERS,
        (AssemblyProteins.Source.ASSEMBLY_ANALYSIS, "6.2"): V6_GENE_CALLERS,
    }

    id = models.SmallAutoField(primary_key=True)
    name = models.TextField()
    version = models.TextField()

    class Meta(MGnifyProteinsBase.Meta):
        db_table = "gene_caller"
        constraints = [
            models.UniqueConstraint(
                fields=["name", "version"], name="gene_caller_name_version"
            )
        ]

    def __str__(self):
        return f"{self.name} {self.version}"

    @classmethod
    def of_pipeline_version(
        cls,
        pipeline_version: str,
        source: AssemblyProteins.Source = AssemblyProteins.Source.ASSEMBLY_ANALYSIS,
    ) -> GeneCallers:
        """The gene callers of a pipeline version.

        :param pipeline_version: major.minor, for example "6.1".
        :param source: The pipeline that predicted the genes.
        :return: Each gene caller name, and whether its headers carry a partial= flag.
        :raises UnsupportedPipelineVersion: When the version is not known.
        """
        try:
            return cls.BY_PIPELINE_VERSION[source, pipeline_version]
        except KeyError:
            raise UnsupportedPipelineVersion(
                f"The gene callers of {source} pipeline version {pipeline_version} are not known"
            ) from None


# Staged rows: the output of the protein ingestion that is not yet in the protein dataset.
# The accessioning core writes them, in one transaction for each assembly proteins record.
# The dataset update copies them to the Parquet files, then deletes them in the same
# transaction that marks the day done. So these tables are empty after each dataset update.
# The accessioning core writes contigs and occurrences with StagedQuerySet.copy_rows().


class StagedQuerySet(models.QuerySet):
    def copy_rows(self, fields: list[str], rows: Iterable[tuple]) -> None:
        """Writes rows with COPY. The database gives the ids and staged_at.

        Raw SQL, for speed: for 200,000 occurrences, bulk_create() took about 7x as long.

        :param fields: The model's field names, in the order of each row.
        :param rows: Tuples of values, in the order of fields.
        """
        connection = connections[self.db]
        table = connection.ops.quote_name(self.model._meta.db_table)
        columns = ", ".join(
            connection.ops.quote_name(self.model._meta.get_field(field).column)
            for field in fields
        )
        with connection.cursor() as cursor:
            with cursor.copy(f"COPY {table} ({columns}) FROM STDIN") as copy:
                for row in rows:
                    copy.write_row(row)


class StagedProtein(MGnifyProteinsBase):
    """A new protein, with its MGYP id and sequence, that waits for the next dataset update."""

    id = models.BigIntegerField(primary_key=True)  # the protein id
    hash = models.BinaryField()
    sequence = models.TextField()
    assembly_proteins = models.ForeignKey(AssemblyProteins, on_delete=models.DO_NOTHING)
    staged_at = models.DateTimeField(db_default=Now())

    class Meta(MGnifyProteinsBase.Meta):
        db_table = "staged_protein"


class StagedContig(MGnifyProteinsBase):
    """A contig of an ingested assembly, that waits for the next dataset update."""

    id = models.BigAutoField(primary_key=True)  # from contig_id_seq
    assembly_proteins = models.ForeignKey(AssemblyProteins, on_delete=models.DO_NOTHING)
    name = models.TextField()  # the pipeline's name, for example ERZ101_17
    original_name = models.TextField(
        null=True
    )  # the submitted name; empty when unknown
    length = models.IntegerField()
    hash = models.BinaryField()
    kmer_coverage = models.FloatField(null=True)
    staged_at = models.DateTimeField(db_default=Now())

    objects = StagedQuerySet.as_manager()

    class Meta(MGnifyProteinsBase.Meta):
        db_table = "staged_contig"


class StagedOccurrence(MGnifyProteinsBase):
    """One predicted gene: one protein at one position on one contig, from one gene caller. It waits for the next dataset update."""

    id = models.BigAutoField(primary_key=True)  # from occurrence_id_seq
    assembly_proteins = models.ForeignKey(AssemblyProteins, on_delete=models.DO_NOTHING)
    gene_id = models.TextField()
    # Not a ForeignKey, since Protein is identified by its hash.
    protein_id = models.BigIntegerField()
    contig = models.ForeignKey(StagedContig, on_delete=models.DO_NOTHING)
    gene_caller = models.ForeignKey(GeneCaller, on_delete=models.DO_NOTHING)
    start_position = models.IntegerField()
    end_position = models.IntegerField()
    strand = models.SmallIntegerField()  # -1 or 1
    truncation = models.TextField(null=True)  # Pyrodigal's partial=XY
    staged_at = models.DateTimeField(db_default=Now())

    objects = StagedQuerySet.as_manager()

    class Meta(MGnifyProteinsBase.Meta):
        db_table = "staged_occurrence"


class DatasetUpdate(MGnifyProteinsBase):
    """One run of the dataset update flow."""

    class Status(models.TextChoices):
        RUNNING = "running"
        DONE = "done"
        FAILED = "failed"

    id = models.BigAutoField(primary_key=True)
    day = models.DateField()
    status = models.TextField(choices=Status)
    started_at = models.DateTimeField()
    finished_at = models.DateTimeField(null=True)
    assembly_proteins = models.IntegerField(null=True)
    proteins = models.BigIntegerField(null=True)
    contigs = models.BigIntegerField(null=True)
    occurrences = models.BigIntegerField(null=True)
    message = models.TextField(null=True)

    class Meta(MGnifyProteinsBase.Meta):
        db_table = "dataset_update"

    def __str__(self):
        return f"Dataset update {self.day} ({self.status})"


class DatasetOwner(MGnifyProteinsBase):
    """The flow run that may write the protein dataset. One row, whose columns are NULL when no flow run owns it."""

    class Flow(models.TextChoices):
        UPDATE = "update"
        MERGE = "merge"

    id = models.BooleanField(primary_key=True, default=True)
    flow = models.TextField(choices=Flow, null=True)
    slurm_job_id = models.BigIntegerField(null=True)
    slurm_submit_at = models.DateTimeField(null=True)
    execution_id = models.UUIDField(null=True)
    flow_run_id = models.UUIDField(null=True)
    acquired_at = models.DateTimeField(null=True)

    class Meta(MGnifyProteinsBase.Meta):
        db_table = "dataset_owner"

    def __str__(self):
        return f"Protein dataset owner: {self.flow or 'none'}"
