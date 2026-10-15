# proteins: the MGnify proteins app

Gives an MGYP accession to every protein of the assemblies, and keeps all proteins, contigs and occurrences for protein releases.

Proteins belong to an assembly analysis. The unit is an **assembly proteins** record: the proteins of one assembly (ERZ), for one assembly analysis (MGYA), predicted by one version (`major.minor`) of the source pipeline. Every record has an MGYA. Study, biome and privacy flags come through the MGYA.

The source is the pipeline that called the genes. Today it is always `assembly_analysis`: ASA calls the genes, and protein ingestion reads the outputs of annotated analyses, new ones by trigger, old ones by hand. `gene_calling` is reserved for gene calling before ASA, which is not built yet. Each source has its own version numbers, so the source is part of the record's key.

## Parts

| Part | What it is |
|---|---|
| MGYP database | PostgreSQL, Django alias `mgyp`, schema `proteindb`. It holds the hash and id of each protein, the assembly proteins records, the gene callers and the staged rows. `sql/mgyp_schema.sql` creates it, not Django migrations. The models in `models.py` are unmanaged. |
| Protein dataset | Parquet files in `EMG_CONFIG.proteins.dataset_dir`. All proteins, contigs and occurrences, and one metadata snapshot for each day. Protein releases read it. |
| Accessioning core | `accession/`: the hash contract, the file reader, the MGYP lookup and the staging transaction. It does not import Django. |

| Path | |
|---|---|
| `sql/mgyp_schema.sql`, `sql/mgyp_roles.sql` | The schema and its grants, and the role settings |
| `apps.py`, `models.py`, `admin.py`, `routers.py` | Django: the system check, unmanaged models, read-only admin, the router and the `mgyp` connection |
| `accession/` | The accessioning core (no Django) |
| `ingestion/analyses.py` | Pending analyses, chunks, and the ingestion of one analysis |
| `dataset/layout.py`, `dataset/metadata.py` | The protein dataset layout, and the metadata snapshot |
| `dataset/update.py`, `dataset/merge.py` | The steps of the dataset update and of the merge |
| `dataset/status.py` | The state and its checks |
| `workflows/flows/proteins/` | The Prefect flows. Prefect runs all the work: there are no management commands. |

`Protein.objects.find_mgyps(["MKV…"])` gives the MGYP of each sequence that has one.

## Settings and roles

| Setting | |
|---|---|
| `MGYP_DATABASE_URL` | The `mgyp` alias, as `proteindb_accession`. Without it, the alias does not exist, and `manage.py check` shows warning `proteins.W001`. |
| `EMG_PROTEINS__DATASET_DIR` | The protein dataset. The jobs of the `slurm` work pool must be able to create and delete files in it. |
| `EMG_PROTEINS__DUCKDB_MEMORY_LIMIT` | The memory limit of DuckDB when it sorts, `32GB`. DuckDB goes over it, so keep it about half of the job memory. |
| `EMG_PROTEINS__INGESTION_CHUNK_SIZE` | Analyses in one chunk flow run, `50`. |
| `EMG_PROTEINS__CONNECTION_WAIT_SECONDS` | How long a connection to the MGYP database waits while all connections are taken, `600`. |
| `EMG_PROTEINS__DATASET_CONCURRENCY_LIMIT` | The Prefect global concurrency limit that the dataset update and the merge share, `protein-dataset`. |

| Role | Used by |
|---|---|
| `proteindb_owner` | `mgyp_schema.sql`, and the migration of the current protein database |
| `proteindb_accession` | emgapi-v2's `mgyp` alias: protein ingestion, dataset update, merge, admin |
| `proteindb_read` | People, for queries by hand |
| `proteindb_load` | Unused. It has only `USAGE` on the schema |

## Setting up an environment

1. As `proteindb_owner`: `psql "<owner URI>" -v ON_ERROR_STOP=1 --single-transaction -f proteins/sql/mgyp_schema.sql`.
2. As a role that can alter the roles: `mgyp_roles.sql`, in the same way. Until it is applied, add `?options=-c%20search_path%3Dproteindb` to the database URL.
3. Create the concurrency limit on the Prefect server of the environment: `prefect gcl create protein-dataset --limit 1`. Without it, the dataset update and the merge fail at start.
4. Set the settings above in the `env` of the base job template of the `slurm` work pool, and register the deployments.

## Flows

| Deployment | Start | Does |
|---|---|---|
| `ingest_assembly_proteins_deployment` | Trigger on the completion of `sync_assembly_batch_results_deployment`, or by hand | Lists the pending analyses (state `ANALYSIS_ANNOTATIONS_IMPORTED`, a pipeline version with gene-caller rules, no assembly proteins record yet, the lowest id of each ERZ and version), and starts one chunk flow for each chunk of one study. Does not wait. |
| `ingest_assembly_proteins_chunk_deployment` | Started by the flow above | Ingests each analysis of the chunk. A failed analysis does not stop the others. |
| `ingest_analysis_proteins_deployment` | By hand | Ingests one MGYA, or every annotated analysis of one ERZ. |
| `update_protein_dataset_deployment` | 02:00 UTC | Moves the staged rows into a new day of the protein dataset. Holds the `protein-dataset` limit. Only adds files. |
| `merge_protein_dataset_deployment` | 04:00 UTC on the 1st | Merges the base file and the day files of each hash prefix. Holds the `protein-dataset` limit. |
| `check_proteins_status_deployment` | 08:00 UTC | Logs the state, and fails if any check fails. |
| `export_assembly_analyses_deployment` | By hand | Writes every assembly analysis, by ERZ and pipeline version, with its MGYA and metadata, to a Parquet file. For the migration. |

The schedules and the trigger are inactive until cutover.

`check_proteins_status` logs the state, and each failed check. Run the deployment by hand to see the state now. The checks fail when:

- the last dataset update finished more than 36 h ago, or there has been none;
- a staged row is more than 48 h old;
- the protein dataset has an incomplete day before the last dataset update;
- protein ids are above 90 % of 10¹².

**During a release**, do not run the merge. The merge is the only flow that deletes the files of a complete day.

## Failures

| Failure | Do |
|---|---|
| An analysis fails in a chunk | Read its error in the chunk flow run. Fix the cause, then run `ingest_analysis_proteins` for that MGYA, or `ingest_assembly_proteins` again. |
| `InvalidInput` | Investigate the gene caller output of that analysis. Nothing was written. |
| `IncompleteRerun` | Find out why a second run of the same ERZ and `major.minor` predicted proteins that the first did not. Nothing was written. |
| `UnsupportedPipelineVersion`, `NotImplementedError` | The pipeline version of the analysis has no rule yet (`GeneCaller.BY_PIPELINE_VERSION`, `Analysis.predicted_genes_files()`). |
| A dataset update failed | Run it again, or wait for the next run. Its first step deletes what a failed update left, and marks a committed day complete. |
| `MissingAnalyses` in the dataset update | An MGYA of the MGYP database is not in emgapi-v2. Find out why the analysis was deleted. |
| A file of the protein dataset is lost or corrupted | Restore it from the backup. Files are never changed after they are written. |
| The MGYP database lost commits that the dataset update has not read | No file has those MGYPs. Run `ingest_assembly_proteins` again: it finds the records that were lost. |
| The MGYP database is lost | Restore it from the PostgreSQL backups, then run `ingest_assembly_proteins` again. |
| A dataset update fails with "The protein dataset is ahead of the MGYP database" | The restored database is older than the last done dataset update, so it lost MGYPs that the protein dataset already has. Stop the protein ingestion, because it could give new MGYPs to those proteins. Restore to a later point if you can. If you cannot, the team decides what to do. |

### Only one writer

The dataset update and the merge share one Prefect global concurrency limit, `protein-dataset`, with a limit of 1. So they never write the protein dataset at the same time. There is no guard against a process that Prefect marks Crashed while its SLURM job still runs: check with `sacct` before you start a flow again after a crash.

## Tests

`pytest proteins`. The tests cover the hash contract, the file readers, and that the accessioning core does not import Django.
