# proteins: the MGnify Protein DB

Gives every protein predicted by the assembly analysis pipeline its MGYP accession, and keeps the proteins, their contigs and their occurrences for releases.

- **Tier 1** is a PostgreSQL database, the `proteindb` alias. It holds the sha256 of each protein sequence with its id, in `protein_key`, the registries, and the rows staged since the last load.
- **Tier 2** is Parquet under `$PROTEIN_DB/tier2/`, one directory per table, written one day at a time. It holds everything, and releases read it.
- **`mgyp-accession`** (`accession/`, its own image) runs in the pipeline: it looks up each protein's MGYP, allocates the new ones and stages the analysis's rows, in one transaction, and writes the protein FASTA with MGYPs.
- **The daily load** moves what is staged into a new day of Tier 2, and compacts Tier 2 when due.

`$PROTEIN_DB` below is `EMG_CONFIG.proteindb.root` (`EMG_PROTEINDB__ROOT`).

| Path | |
|---|---|
| `sql/tier1.sql`, `sql/roles.sql` | Tier 1's schema, and the runtime roles' settings |
| `accession/` | `mgyp-accession` and the hash contract |
| `tier2.py`, `load.py`, `dims.py`, `owner.py` | Tier 2's files, the load, the dimension snapshot, and ownership of Tier 2 |
| `status.py` | The state and its checks |
| `migrate.py`, `backfill.py`, `backfill_task.sh` | The migration from the current database, and the backfill |
| `flows/` | `proteindb_load`, `proteindb_check`, `proteindb_backfill` |
| `management/commands/` | `proteindb_status`, `proteindb_migrate` |

## Roles and configuration

| Role | Used by |
|---|---|
| `proteindb_owner` | Applying the schema, and `proteindb_migrate` |
| `proteindb_accession` | `mgyp-accession`, in the pipeline and the backfill |
| `proteindb_load` | The `proteindb` alias: the load, the check, `proteindb_status` |
| `proteindb_read` | Ad-hoc reads of the DB by people |

| Setting | |
|---|---|
| `PROTEINDB_DATABASE_URL` | The `proteindb` alias, as `proteindb_load` (as `proteindb_owner` for `proteindb_migrate`). Without it the alias does not exist |
| `PROTEINDB_DSN` | libpq URI as `proteindb_accession`, for `mgyp-accession`. The pipeline takes it as a Nextflow secret; the backfill's tasks inherit it from the flow run |
| `EMG_PROTEINDB__ROOT` | `$PROTEIN_DB`. The `slurm` work pool's jobs must be able to create and delete files under `tier2/` |
| `EMG_PROTEINDB__ACCESSION_IMAGE` | The `mgyp-accession` image the pipeline pins, as `docker://quay.io/microbiome-informatics/mgyp-accession:<tag>`, for the backfill. Move it with the pipeline's pin |
| `EMG_PROTEINDB__TIER2_CONCURRENCY_LIMIT` | The Prefect limit that keeps loads apart, `proteindb-tier2` |
| `EMG_PROTEINDB__SORT_MEMORY_LIMIT` | DuckDB's memory limit when it sorts Tier 2's rows, `32GB`. DuckDB goes over it by about 1 GB, so keep it about half of the job's memory |

## Setting up an environment

1. Apply the schema as `proteindb_owner`: `psql "<owner URI>" -v ON_ERROR_STOP=1 --single-transaction -f proteins/sql/tier1.sql`, then `roles.sql` the same way, as a role that may alter the runtime roles. Neither should print a warning.
2. Check the roles on fresh connections: as each runtime role, `SHOW search_path` gives `proteindb`.
3. Create the concurrency limit against the environment's Prefect server: `prefect gcl create proteindb-tier2 --limit 1`. `prefect gcl inspect proteindb-tier2` must show it active with a limit of 1, or the load refuses to run.
4. Set the configuration above in the `env` of the `slurm` work pool's base job template, and register the deployments with the rest of the environment's deployment file. In production the schedules of the load and the check are defined inactive until cutover.

## Daily operation

| Deployment | Schedule (UTC) | Does |
|---|---|---|
| `proteindb_load_deployment` | 02:00 | The load. One run at a time owns Tier 2; it has no Prefect retries |
| `proteindb_check_deployment` | 08:00 | Logs the state, and fails if any check fails |
| `proteindb_backfill_deployment` | by hand | See [Backfill](#backfill) |

`python manage.py proteindb_status` prints the state, and each failed check. `--check` makes it exit non-zero if any fails. The checks fail when:

- the last successful load finished more than 36 h ago, or there has been none;
- a staged row is more than 48 h old;
- Tier 2 has an owner whose SLURM job is not running;
- Tier 2 has an incomplete day before the last load's;
- protein ids are above 90 % of 10¹².

A failed run of any deployment is seen in Prefect. Nothing else notifies anyone.

**During a release**, pause the schedule of `proteindb_load_deployment` while the release reads Tier 2, and do not start a load by hand. Nothing is lost: accessioning keeps staging, and the next load takes everything. A schedule left paused shows as an overdue load after 36 h.

## Failures

| Failure | Do |
|---|---|
| `mgyp-accession` exits 1 (database unreachable, timeout, killed) | Nextflow retries it. A retry after a commit writes the same output. If every retry fails, resume the run with `-resume` once the cause is fixed |
| `mgyp-accession` exits 2 (invalid input) | Investigate that assembly's gene-caller output. Nothing was written |
| `mgyp-accession` exits 3 (registered analysis with proteins that have no MGYP) | Find out why a second run of the same assembly and `major.minor` predicted proteins the first did not. Nothing was written |
| A load failed | Rerun it, or wait for the next scheduled run. Its first step cleans up what a failed load left, and publishes a day that was committed but not published |
| A load fails at start with "Tier 2 is owned by job N" | [Clear ownership of Tier 2](#clearing-ownership-of-tier-2) |
| An assembly is unresolved | `proteindb_status` lists it. Its occurrences are withheld from releases until emgapi-v2 knows it, which takes effect at the next load |
| A Tier 2 file is lost or corrupted | Restore it from the `/nfs/production` backup. Files are never modified once written, so the restored one is correct |
| Tier 1 restored by point-in-time recovery that lost, or may have lost, commits | [Restart allocation](#restarting-allocation-after-lost-commits) before accessioning resumes |
| Tier 1 lost, with no recovery | [Restart allocation](#restarting-allocation-after-lost-commits), rebuilding Tier 1 from Tier 2 |
| A load fails at start with "Tier 2 has days that load_log has not done" | Tier 1 was restored to before those loads. Their rows are staged again, and Tier 1's registries and id sequences are behind Tier 2. Treat Tier 1 as lost: [restart allocation](#restarting-allocation-after-lost-commits), rebuilding Tier 1 from Tier 2 |

### Clearing ownership of Tier 2

The recorded owner's job has not visibly ended normally. No load may start until that job is known to have stopped, because a stalled process that resumes could overwrite a day another load has committed.

1. `sacct -j N`. If the job is running and should not be, `scancel N` and wait until `sacct` shows it ended. If it ended `COMPLETED`, `FAILED`, `CANCELLED`, `TIMEOUT` or `OUT_OF_MEMORY`, the next load takes over by itself.
2. If the state is `NODE_FAIL` or unclear, ask the cluster team to confirm that the node is down or that the job's processes are gone.
3. Only then, with the load's schedule paused: `UPDATE proteindb.tier2_owner SET flow = NULL, slurm_job_id = NULL, slurm_submit_at = NULL, execution_id = NULL, flow_run_id = NULL, acquired_at = NULL WHERE execution_id = '<the recorded one>'`. If the Prefect slot is still taken, `prefect gcl update proteindb-tier2 --active-slots 0`. Resume the schedule.
4. Note what was checked in the team's log.

Never clear the row while the old job might still be running.

### Restarting allocation after lost commits

Pipeline outputs may carry MGYPs that Tier 1 no longer knows. `protein_id_seq` must restart above every one of them, or they would be issued again to other proteins. The sequence's position cannot tell how far: inserts that conflicted or rolled back took values that are never used. `mgyp-accession` writes an MGYP only after its commit, and only into its output FASTA, so the restart point is read from the outputs.

The **checkpoint** is the restore point after a point-in-time recovery, or 00:00 UTC of the latest complete Tier 2 day for a rebuild.

1. **Stop accessioning, and wait for it to stop.** Pause pipeline and backfill submissions. `accession_proteins = false` affects only runs started afterwards, so stop the running ones that could still start an `MGYP_ACCESSION` task. Check the SLURM queue and the Nextflow run history until no `MGYP_ACCESSION` or backfill task is pending or running: a task writes its FASTA after closing its connection, so `pg_stat_activity` is not enough.
2. **List every output FASTA written by a task running at or after the checkpoint** into `outputs.txt`, one path a line:
   - pipeline runs, from their Nextflow history, including runs that started before the checkpoint, and runs that failed after `MGYP_ACCESSION` succeeded, whose output is in the task's work directory;
   - backfill tasks, from the `tasks.tsv` files under `$PROTEIN_DB/backfill/runs/`;
   - earlier recoveries, under `$PROTEIN_DB/recovery/`.
3. **Read the highest MGYP:**

   ```
   set -o pipefail; xargs -a outputs.txt zcat -f | grep -o '^>[^ ]* MGYP[0-9]\{12\}' | grep -o 'MGYP[0-9]*' | sort | tail -n 1
   ```

   If it fails, a file is missing or unreadable, and the result is not used.
4. **Restart the sequence** after the greatest of that MGYP's number and the highest id the surviving database knows.
   - After a restore: also never below the sequence's restored position. `SELECT setval('proteindb.protein_id_seq', N)`, as `proteindb_owner`.
   - For a rebuild: drop the schema, apply `tier1.sql` and `roles.sql` again, and `python manage.py proteindb_migrate tier1 --rebuild --protein-id-start N+1` as `proteindb_owner`. It refuses a start at or below Tier 2's highest id.
5. **If the list cannot be shown to be complete**, for example because an output was deleted or replaced by a rerun with a different input, accessioning stays stopped. The team decides how far to move the sequence, and records the decision.
6. **Re-accession** the analyses registered after the checkpoint, as the backfill does, with `--out $PROTEIN_DB/recovery/<date>/<accession>/<major.minor>/<accession>_predicted_cds.faa.gz`. `recovery/<date>/` must not exist yet. Never overwrite the earlier outputs: they are the evidence for step 2 next time. Republish the results from `recovery/`.
7. Resume submissions.

## Backfill

`proteindb_backfill_deployment`, run by hand, optionally with `study`, accessions the completed v6 assembly analyses that Tier 1 has not registered, from their published outputs. It needs `PROTEINDB_DSN` and `EMG_PROTEINDB__ACCESSION_IMAGE`.

It writes `$PROTEIN_DB/backfill/runs/<UTC time>/` with `tasks.tsv`, `missing.tsv` (analyses missing an input, left out, to be handled separately) and the tasks' logs under `logs/`. It submits the tasks as SLURM arrays, at most 8 running at once, and ends without waiting. Follow them with `sacct -j <job id>`, with the ids from its log. Once they have ended, run it again with the same parameters: it submits only what is still unregistered. Repeat until it lists none, or only analyses whose tasks fail every time, which are investigated from their logs.

Outputs go to `$PROTEIN_DB/backfill/<accession>/<major.minor>/` and are never deleted: a recovery reads them.

## Migration

From the frozen current database (`mgnprotein_ingestion`), once. Run in emgapi-v2's environment on the cluster, as `proteindb_owner`, with `SRC` the current database's URI, `OUT` the migration directory on `/hps/nobackup`, `M` the date of the freeze, and `TMPDIR` on `/hps/nobackup` for DuckDB to spill to. The resources are first estimates.

1. **Export**, the reference tables and then one array per table, each after the previous one, so that at most 8 tasks read at once:

   ```
   python manage.py proteindb_migrate export --table dims --source "$SRC" --out "$OUT"
   J=$(sbatch --parsable --array=0-63%8 --mem=4G --cpus-per-task=2 --time=4:00:00 [--dependency=afterany:$J] \
       -J export-$T --wrap "python manage.py proteindb_migrate export --table $T --child \$SLURM_ARRAY_TASK_ID --source $SRC --out $OUT")
   ```

   for `T` in `protein`, `occurrence`, `contig`. A child already exported is refused. Rerun any task that failed.
2. **Build Tier 2's first day.** If it ends early, submit it again: it carries on where it stopped.

   ```
   sbatch --mem=64G --cpus-per-task=16 --time=24:00:00 -J tier2 --wrap "python manage.py proteindb_migrate tier2 --date M --export $OUT"
   ```

3. **Build Tier 1**, into a schema just created by `tier1.sql`: `python manage.py proteindb_migrate tier1`. An interrupted build is started over: drop the schema and apply `tier1.sql` and `roles.sql` again.
4. **Verify**: Tier 2 against the frozen tables, Tier 1 against Tier 2, and a sample of the hash contract. It prints each check that fails.

   ```
   sbatch --mem=64G --cpus-per-task=16 --time=24:00:00 -J verify --wrap "python manage.py proteindb_migrate verify --source $SRC"
   ```

   Delete `$OUT` once verified.
5. **Resolve** the assemblies, in emgapi-v2's production environment, any time before cutover. It lists, by pipeline version, every assembly accession of the current database that emgapi-v2 does not know. The first load after cutover takes the dimensions from emgapi-v2, so these would be left out of every release. The team reviews the list before cutover.

   ```
   python manage.py proteindb_migrate resolve --source "$SRC"
   ```

## Tests

`pytest proteins/` in the app's container. They build the Tier 1 schema from `sql/tier1.sql` in the `proteindb` test database, so that file must stay equal to the deployed schema.
