# proteins: the MGnify proteins app

Gives an MGYP accession to every protein of the assemblies, and keeps all proteins, contigs and occurrences for protein releases.

Proteins belong to an assembly analysis. The unit is an **assembly proteins** record: the proteins of one assembly (ERZ), for one assembly analysis (MGYA), predicted by one version (`major.minor`) of the source pipeline. Every record has an MGYA. Study, biome and privacy flags come through the MGYA. There are two sources:

| Source | Path |
|---|---|
| `assembly_analysis` | Assembly analysis. ASA calls the genes. Protein ingestion reads the outputs of annotated analyses: new ones by trigger, old ones by hand. |
| `gene_calling` | Gene calling + assembly analysis. The `gene_call_assembly` flow makes the MGYA, runs [genecalling-pipeline](https://github.com/EBI-Metagenomics/genecalling-pipeline), gives MGYPs to the proteins, writes the FAA with MGYPs, and starts the assembly analysis of that MGYA. The tracker is `workflows.AssemblyGeneCalling`. The trigger path skips these analyses. |

## Parts

| Part | What it is |
|---|---|
| MGYP database | PostgreSQL, Django alias `mgyp`, schema `mgyp`. It holds the hash and id of each protein, the assembly proteins records, the gene callers and the staged rows. `sql/mgyp_schema.sql` creates it, not Django migrations. The models in `models.py` are unmanaged. |
| Protein dataset | Parquet files in `EMG_CONFIG.proteins.dataset_dir`. All proteins, contigs and occurrences, and one metadata snapshot for each day. Protein releases read it. |
| Accessioning core | `accession/`: the hash contract, the file reader, the MGYP lookup and the staging transaction. It does not import Django. |

| Path | |
|---|---|
| `sql/mgyp_schema.sql`, `sql/mgyp_roles.sql` | The schema, and the role settings |
| `apps.py`, `models.py`, `admin.py`, `routers.py` | Django: the system check, unmanaged models, read-only admin, the router and the `mgyp` connection string |
| `accession/` | The accessioning core (no Django) |
| `ingestion/analyses.py` | Pending analyses, chunks, and the ingestion of one analysis (source `assembly_analysis`) |
| `ingestion/gene_calling.py` | The accessioning of one gene calling, and the FAA with MGYPs (source `gene_calling`) |
| `workflows.models.AssemblyGeneCalling` | The tracker of the gene calling path, in the emgapi-v2 database |
| `dataset/layout.py`, `dataset/metadata.py` | The protein dataset layout, and the metadata snapshot |
| `dataset/update.py`, `dataset/merge.py` | The steps of the dataset update and of the merge |
| `dataset/status.py` | The state and its checks |
| `workflows/flows/proteins/` | The Prefect flows. Prefect runs all the work: there are no management commands. |

`Protein.objects.find_mgyps(["MKV…"])` gives the MGYP of each sequence that has one.

## Settings and roles

| Setting | |
|---|---|
| `MGYP_DATABASE_URL` | The `mgyp` alias, as `mgyp_emgapi`. Without it, the alias does not exist, and `manage.py check` shows warning `proteins.W001`. |
| `EMG_PROTEINS__DATASET_DIR` | The protein dataset. The jobs of the `slurm` work pool must be able to create and delete files in it. |
| `EMG_PROTEINS__DUCKDB_MEMORY_LIMIT` | The memory limit of DuckDB when it sorts, `32GB`. DuckDB goes over it, so keep it about half of the job memory. |
| `EMG_PROTEINS__INGESTION_CHUNK_SIZE` | Analyses in one chunk flow run, `50`. |
| `EMG_PROTEINS__DATASET_CONCURRENCY_LIMIT` | The Prefect global concurrency limit that the dataset update and the merge share, `protein-dataset`. |
| `EMG_PROTEINS__ACCESSIONING_CONCURRENCY_LIMIT` | The Prefect global concurrency limit on the accessioning step of `gene_call_assembly`, `protein-accessioning`. Each step uses up to 16 connections. |
| `EMG_GENE_CALLING_PIPELINE__*` | The gene calling pipeline: repository, revision, version (`major.minor`), output folders. |

| Role | Used by |
|---|---|
| `mgyp_owner` | `mgyp_schema.sql`, and the legacy import once |
| `mgyp_emgapi` | emgapi-v2: protein ingestion, dataset update, merge, admin, commands |
| `mgyp_read` | People, for queries by hand |

## Setting up an environment

1. As `mgyp_owner`: `psql "<owner URI>" -v ON_ERROR_STOP=1 --single-transaction -f proteins/sql/mgyp_schema.sql`.
2. As a role that can alter the roles: `mgyp_roles.sql`, in the same way. Until it is applied, add `?options=-c%20search_path%3Dmgyp` to the database URL.
3. Create the concurrency limits on the Prefect server of the environment: `prefect gcl create protein-dataset --limit 1` and `prefect gcl create protein-accessioning --limit 8`. Without them, the flows that use them fail at start.
4. Set the settings above in the `env` of the base job template of the `slurm` work pool, and register the deployments.

## Flows

| Deployment | Start | Does |
|---|---|---|
| `ingest_assembly_proteins_deployment` | Trigger on the completion of `sync_assembly_batch_results_deployment`, or by hand | Lists the pending analyses (state `ANALYSIS_ANNOTATIONS_IMPORTED`, no assembly proteins record yet), and starts one chunk flow for each chunk of one study. Does not wait. |
| `ingest_assembly_proteins_chunk_deployment` | Started by the flow above | Ingests each analysis of the chunk. A failed analysis does not stop the others. |
| `ingest_analysis_proteins_deployment` | By hand | Ingests one MGYA, or every annotated analysis of one ERZ. |
| `gene_call_assembly_deployment` | By hand, for one ERZ | Gene calling + assembly analysis: makes the MGYA, downloads the assembly, runs genecalling-pipeline, gives MGYPs to the proteins, writes the FAA with MGYPs, then starts `run_assembly_batch_deployment` for the MGYA. Skips completed steps. `analysis_assembly_study` leaves these analyses out of its batches. |
| `update_protein_dataset_deployment` | 02:00 UTC | Moves the staged rows into a new day of the protein dataset. Holds the `protein-dataset` limit. Only adds files. |
| `merge_protein_dataset_deployment` | 04:00 UTC on the 1st | Merges the base file and the day files of each hash prefix. Holds the `protein-dataset` limit. |
| `check_proteins_status_deployment` | 08:00 UTC | Logs the state, and fails if any check fails. |
| `export_assembly_analyses_deployment` | By hand | Writes every assembly analysis, by ERZ and pipeline version, with its MGYA and metadata, to a Parquet file. For the legacy import. |

The schedules are inactive until cutover.

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
| The gene calling pipeline fails | `gene_calling_status` is FAILED, with the error in `last_error`. Fix the cause and run `gene_call_assembly` again: Nextflow resumes. |
| The accessioning step fails | `accessioning_status` is FAILED. Run `gene_call_assembly` again. A second run gives the same MGYPs and writes the same FAA. |
| `GeneCallingAlreadyRunning` | Another flow run has a step of the same gene calling running. After a crash, check with `sacct` that its job has ended, then set the status to FAILED in the admin. |
| `GeneCallingRefused` | The V6 analysis of the assembly has started without a gene calling (it is in a batch, or its annotations are imported), or has a gene calling of another version. |
| A file of the protein dataset is lost or corrupted | Restore it from the backup. Files are never changed after they are written. |
| The MGYP database lost commits that the dataset update has not read | Source `assembly_analysis`: nobody has seen those MGYPs. Run `ingest_assembly_proteins` again: it finds the records that were lost. Source `gene_calling`: the FAA files with MGYPs have them. Do the `protein_id_seq` step of the next row before any new accessioning. |
| The MGYP database is lost | Restore it from the PostgreSQL backups. Then, if gene callings ran after the backup, set `protein_id_seq` above the highest MGYP in their FAA files with MGYPs: those MGYPs left the database before any dataset update. TODO: code for this step. |
| A dataset update fails with "The protein dataset is ahead of the MGYP database" | The restored database is older than the last done dataset update, so it lost MGYPs that the protein dataset already has. Stop the protein ingestion, because it could give new MGYPs to those proteins. Restore to a later point if you can. If you cannot, the team decides what to do. |

### Only one writer

The dataset update and the merge share one Prefect global concurrency limit, `protein-dataset`, with a limit of 1. So they never write the protein dataset at the same time. There is no guard against a process that Prefect marks Crashed while its SLURM job still runs: check with `sacct` before you start a flow again after a crash.

## Legacy import

The legacy import copies the frozen legacy protein DB into the first day of the protein dataset, once. It is not part of emgapi-v2. It needs two things from emgapi-v2:

1. The file of `export_assembly_analyses_deployment`. The import gives each legacy assembly the MGYA of the emgapi-v2 analysis with the same ERZ and pipeline version, and stops if one has none.
2. The empty MGYP database. The import fills it, as `mgyp_owner`, from the dataset that it has just written.

## Tests

There are no tests yet. The tests of the previous prototype were removed, because they did not match this design.
