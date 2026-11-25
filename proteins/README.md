# proteins: the MGnify proteins app

Gives an MGYP accession to every protein of the assemblies, and keeps all proteins, contigs and occurrences for protein releases.

Proteins belong to an assembly analysis. The unit is an **assembly proteins** record: the proteins of one assembly (ERZ), for one assembly analysis (MGYA), predicted by one version (`major.minor`) of the source pipeline. Every record has an MGYA. Study, biome and privacy flags come through the MGYA.

The source is the pipeline that called the genes. Today it is always `assembly_analysis`: ASA calls the genes, and protein ingestion reads the outputs of annotated analyses, new ones by trigger, old ones by hand. `gene_calling` is reserved for gene calling before ASA, which is not built yet. Each source has its own version numbers, so the source is part of the record's key.

## Parts

| Part | What it is |
|---|---|
| MGYP database | PostgreSQL, Django alias `mgyp`, schema `proteindb`. It holds the hash and id of each protein, the assembly proteins records, the gene callers, the staged rows, the dataset updates and the owner of the protein dataset. `sql/mgyp_schema.sql` creates it, not Django migrations. The models in `models.py` are unmanaged. |
| Protein dataset | Parquet files in `EMG_CONFIG.proteins.dataset_dir`. All proteins, contigs and occurrences, and one metadata snapshot for each day. Protein releases read it. |
| Accessioning core | `accessioning/`: the hash contract, reading the predicted genes, the MGYP lookup and the staging transaction. It uses the models; the raw SQL is in `ProteinQuerySet.allocate()` and `StagedQuerySet.copy_rows()`. |

| Path | |
|---|---|
| `sql/mgyp_schema.sql`, `sql/mgyp_roles.sql` | The schema and its grants, and the role settings |
| `apps.py`, `models.py`, `admin.py`, `routers.py` | Django: the system check, unmanaged models, read-only admin, the router |
| `accessioning/` | The accessioning core |
| `readers.py` | The FASTA and GFF readers |
| `ingestion/assembly_analyses.py` | Pending analyses, chunks, and the ingestion of one analysis |
| `dataset/parquet_files.py`, `dataset/export/metadata_snapshot.py` | The protein dataset files, and the metadata snapshot |
| `dataset/export/daily_update.py`, `dataset/export/monthly_merge.py` | The steps of the dataset update and of the merge |
| `dataset/owner.py` | Ownership of the protein dataset: the limit, the slot and the owner record |
| `status.py` | The state and its checks |
| `dataset/build.py`, `management/commands/build_mgyp_database.py` | Building the MGYP database from the protein dataset, after the migration and to recover a lost database. A command, because it runs as `proteindb_owner` |
| `workflows/flows/proteins/` | The Prefect flows, which run the rest of the work |

`Protein.objects.find_mgyps(["MKV…"])` gives the MGYP of each sequence that has one.

## Settings and roles

| Setting | |
|---|---|
| `MGYP_DATABASE_URL` | The `mgyp` alias, as `proteindb_accession`. Without it, the alias does not exist, and `manage.py check` shows warning `proteins.W001`. |
| `EMG_PROTEINS__DATASET_DIR` | The protein dataset. The jobs of the `slurm` work pool must be able to create and delete files in it. |
| `EMG_PROTEINS__TMP_DIR` | The dataset update's scratch, and where DuckDB spills while it sorts, on `/hps/nobackup`: up to ~22 GB for one prefix of the merge. It must exist, and the jobs of the `slurm` work pool must be able to create and delete files in it. |
| `EMG_PROTEINS__DUCKDB_MEMORY_LIMIT` | The memory limit of DuckDB when it sorts, `32GB`. DuckDB goes over it, so keep it about half of the job memory. |
| `EMG_PROTEINS__INGESTION_CHUNK_SIZE` | Analyses in one chunk flow run, `50`. |
| `EMG_PROTEINS__DATASET_CONCURRENCY_LIMIT` | The Prefect global concurrency limit that the dataset update and the merge share, `protein-dataset`. |

| Role | Used by |
|---|---|
| `proteindb_owner` | `mgyp_schema.sql`, `build_mgyp_database` |
| `proteindb_accession` | emgapi-v2's `mgyp` alias: protein ingestion, dataset update, merge, admin |
| `proteindb_read` | People, for queries by hand |
| `proteindb_load` | Unused. It has only `USAGE` on the schema |

## Setting up an environment

1. As `proteindb_owner`: `psql "<owner URI>" -v ON_ERROR_STOP=1 --single-transaction -f proteins/sql/mgyp_schema.sql`.
2. As a role that can alter the roles: `mgyp_roles.sql`, in the same way. Until it is applied, add `?options=-c%20search_path%3Dproteindb` to the database URL.
3. In production, once the migration has written the protein dataset's first day, as `proteindb_owner`: `python manage.py build_mgyp_database --dsn "host=H dbname=D user=proteindb_owner options='-c search_path=proteindb'"`. The DSN names the schema, since `proteindb_owner` has no `search_path` of its own. It fills the MGYP database from the protein dataset, and refuses a schema that is not empty, so an interrupted build starts again from step 1, after `DROP SCHEMA proteindb CASCADE`.
4. Create the concurrency limit on the Prefect server of the environment: `prefect gcl create protein-dataset --limit 1`. Without it, the dataset update and the merge fail at start.
5. Set the settings above in the `env` of the base job template of the `slurm` work pool, and register the deployments.

## Flows

| Deployment | Start | Does |
|---|---|---|
| `ingest_assembly_proteins_deployment` | Trigger on the completion of the `copy_assembly_batch_results` flow, which `run_assembly_batch` runs at its end, or by hand | Lists the pending analyses (state `ANALYSIS_ANNOTATIONS_IMPORTED`, a pipeline version with gene-caller rules, no assembly proteins record yet, the lowest id of each ERZ and version), and starts one chunk flow for each chunk of one study. Does not wait. |
| `ingest_assembly_proteins_chunk_deployment` | Started by the flow above | Ingests each analysis of the chunk. A failed analysis does not stop the others. |
| `ingest_analysis_proteins_deployment` | By hand | Ingests one MGYA, or every annotated analysis of one ERZ, of a pipeline version with gene-caller rules. |
| `update_protein_dataset_deployment` | 02:00 UTC | Moves the staged rows into a new day of the protein dataset. Holds the `protein-dataset` limit. Only adds files. |
| `merge_protein_dataset_deployment` | 04:00 UTC on the 1st | Merges the base file and the day files of each hash prefix. Holds the `protein-dataset` limit. |
| `check_proteins_status_deployment` | 08:00 UTC | Logs the state, and fails if any check fails. |

The schedules and the trigger are inactive until cutover.

`check_proteins_status` logs the state, and each failed check. Run the deployment by hand to see the state now. The checks fail when:

- the last dataset update finished more than 36 h ago, or there has been none;
- a staged row is more than 48 h old;
- the protein dataset has an incomplete day before the last dataset update;
- protein ids are above 90 % of 10¹²;
- annotated assembly analyses have a pipeline version with no gene-caller rule, other than 4.1 and 5.0, whose records come from the migration. The listing skips them;
- an assembly proteins record has an MGYA that is not in emgapi-v2, so releases publish none of its proteins;
- the protein dataset has an owner whose SLURM job is not running.

**During a release**, pause the schedules of `update_protein_dataset_deployment` and `merge_protein_dataset_deployment`, check that neither is running, and resume both once the release no longer reads the protein dataset. The merge is the only flow that deletes the files of a complete day.

## Failures

| Failure | Do |
|---|---|
| An analysis fails in a chunk | Read its error in the chunk flow run. Fix the cause, then run `ingest_analysis_proteins` for that MGYA, or `ingest_assembly_proteins` again. |
| `InvalidInput` | Investigate the gene caller output of that analysis. Nothing was written. |
| `IncompleteRerun` | Find out why a second run of the same ERZ and `major.minor` predicted proteins that the first did not. Nothing was written. |
| `UnsupportedPipelineVersion`, `NotImplementedError`, or the check reports a pipeline version with no gene-caller rule | The pipeline version of the analysis has no rule yet. Add it to `GeneCaller.BY_PIPELINE_VERSION` and `Analysis.predicted_genes_files()`. |
| A dataset update failed | Run it again, or wait for the next run. Its first step deletes what a failed update left, and marks a committed day complete. |
| The dataset update logs "withholding the records of N MGYAs", and the check reports MGYAs not in emgapi-v2 | Those MGYAs of the MGYP database are not in emgapi-v2, so releases publish none of their proteins. Find out why the analyses were deleted, and restore them: the next update puts them back. |
| A file of the protein dataset is lost or corrupted | Restore it from the backup. Files are never changed after they are written. |
| The MGYP database lost commits that the dataset update has not read | No file has those MGYPs, but someone could have seen them, so they must never be given again. Disable the trigger of `ingest_assembly_proteins_deployment`, and wait until no ingestion run is pending or running. As `proteindb_owner`, skip 10⁸ ids: `SELECT setval('proteindb.protein_id_seq', last_value + 100000000) FROM proteindb.protein_id_seq`. Enable the trigger again and run `ingest_assembly_proteins` once: it finds the records that were lost. |
| The MGYP database is lost | Restore it from the PostgreSQL backups. Unless the restore is known to have lost no commits, continue as for lost commits, above. |
| A dataset update fails with "The protein dataset is ahead of the MGYP database" | The restored database is older than the last done dataset update, so it lost MGYPs that the protein dataset already has. Stop the protein ingestion, because it could give new MGYPs to those proteins. Restore to a later point if you can. If you cannot, rebuild it from the protein dataset, as below. |
| The MGYP database is lost, and cannot be restored | Rebuild it from the protein dataset. Disable the trigger of `ingest_assembly_proteins_deployment`, and wait until no ingestion run is pending or running. As `proteindb_owner`, drop the schema (`DROP SCHEMA proteindb CASCADE`), apply `mgyp_schema.sql` again, and run `python manage.py build_mgyp_database --dsn "host=H dbname=D user=proteindb_owner options='-c search_path=proteindb'" --rebuild`. The lost database could have given MGYPs that no file carries, so protein ids start at the protein dataset's highest plus 10⁸. If a restored database's `protein_id_seq` is higher, or the loss is known to be smaller, give the first id with `--protein-id-start N`. The command refuses an N at or below the protein dataset's highest id. Enable the trigger again and run `ingest_assembly_proteins_deployment` once: the analyses whose records were lost are pending again, and get new MGYPs. |

### Only one writer

The dataset update and the merge share one Prefect global concurrency limit, `protein-dataset`, with a limit of 1. Prefect holds the slot through a lease that the flow's process renews, and frees it when the process cannot reach the Prefect server, or when someone resets the slot, while the SLURM job may still run. So the MGYP database also records the owner in `dataset_owner`: its flow, SLURM job and submit time. The next update or merge starts only when `sacct` shows that job has ended normally, and a flow run that lost ownership exits before it writes again.

For example, SLURM loses contact with the node of an update, whose process still runs. SLURM marks its job `NODE_FAIL`, and its lease expires. Someone runs the update again the same day: it deletes the first run's files of the day, writes the day, and commits, deleting the staged rows. Then the node is back, and the first process overwrites the day's files with the rows it read earlier, so the rows staged in between are in no file. With the owner record, the second run fails at start, naming the first run's job, and the first process exits before its next write.

The `slurm` work pool's worker, prefect-slurm, already counts a run that Prefect shows as pending, running or cancelling, and whose SLURM job has not ended, against its deployment's `concurrency_limit: 1`, so a second update is not even submitted. That does not replace the owner record:

- it does not keep the update and the merge apart;
- it takes `NODE_FAIL` as ended, so it submits the second run of the example;
- it stops counting a run once it has marked it Crashed, which it does as soon as SLURM shows the run's job in any state but running, `NODE_FAIL` and `COMPLETING` included;
- it does not stop a process that lost its slot from writing again.

An update or merge that fails at start with "The protein dataset is owned by job N" found an owner whose job has not visibly ended. The check reports the owner whose job is not running too. Never clear the row while the old job might still run:

1. Run `sacct -j N -X -D -o JobID,State,End,NodeList`. If the job still runs and should not, `scancel N` and wait until `sacct` shows it ended: the next run then takes over by itself.
2. If it ended `NODE_FAIL`, run `scontrol show node <node>`. Its processes are gone if `BootTime` is later than the job's `End`, or if `State` no longer includes `NOT_RESPONDING`: when a node registers again, SLURM kills what is left of its ended jobs. Go to step 5, unless `Reason` is `Kill task failed`.
3. If the node still does not respond and has not restarted, look for signs that the job still runs: files changed since `End` in `EMG_PROTEINS__DATASET_DIR` or `EMG_PROTEINS__TMP_DIR` (`find <dir> -newermt '<End>'`), logs after `End` on the row's `flow_run_id` (`prefect flow-run logs <id>`), or the slot still taken 5 minutes after `End` (`prefect gcl inspect protein-dataset`). None of them proves that the job stopped, so leave the row and repeat step 2 each day. A job that still runs either ends and clears the row itself, or is killed when its node responds again. Ingestion carries on meanwhile.
4. Only as the last resort, if the protein dataset cannot wait for the node or nothing above explains the state, ask the cluster team to make sure that the job's processes are gone, for example by restarting the node.
5. With the schedules paused, clear the row as `proteindb_owner`: `UPDATE proteindb.dataset_owner SET flow = NULL, slurm_job_id = NULL, slurm_submit_at = NULL, execution_id = NULL, flow_run_id = NULL, acquired_at = NULL WHERE execution_id = '<the recorded one>'`. If the Prefect slot is still taken too, reset its active slots to 0 with `prefect gcl update protein-dataset --active-slots 0`. Resume the schedules.

## Tests

`pytest proteins`. The tests cover the hash contract, the file readers, and the accessioning core.
