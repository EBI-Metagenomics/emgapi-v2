# Initial molecule index

## Manual export on Codon

Run this command yourself on a Codon datamover.
The script requires Python 3.10 or later and rsync.
It needs no Django installation or database connection.
It reads production files without changing them.

Transfer these files to the datamover:

- `analyses/utils/export_rhea_index.py`
- Your `rheas.tsv` manifest from the production database.

The existing manifest format remains supported:

```text
analysis_accession	pipeline_version	rhea_path
MGYA01015893	V6	ERP104/ERP104177/ERZ1740/ERZ1740921/V6/assembly/functional-annotation/rhea-reactions/ERZ1740921_proteins2rhea.tsv.gz
```

Use your writable Codon directory in place of `/path/to/codon-workdir`.
Check the FTP root against your production configuration.

```bash
python3 export_rhea_index.py \
  --manifest rheas.tsv \
  --ftp-root /nfs/ftp/public/databases/metagenomics/mgnify_results \
  --workdir /path/to/codon-workdir/rhea-cache \
  --output /path/to/codon-workdir/molecule-index.tsv \
  --limit 50
```

Remove `--limit 50` for the full manifest.
The limit selects the first manifest rows.
It does not produce a statistically representative sample.
For one study, export a study-specific manifest with the query below.

Rsync copies only the listed Rhea files.
The cache retains their relative paths.
Later runs reuse the cache through normal rsync size and modification-time checks.
The script parses one file at a time.
It stores only unique ChEBI participants in memory.
It supports gzip files, plain TSVs, optional headers, and empty tables.

The output contains two columns:

```text
analysis_accession	chebi_ids
MGYA01015893	{15377,15378,16236}
```

The row above illustrates the format only.
Empty molecule sets use `{}`.
Invalid tables stop the export.
Missing source files are skipped before rsync.
`molecule-index.missing.tsv` lists their MGYA accessions and source paths.
Cached copies do not replace missing production files.
Other copy errors still stop the export.
An export with no available source files fails.
A failed export preserves an existing output file.
Transfer `molecule-index.tsv` and `molecule-index.missing.tsv` back to the local machine.
The raw Rhea cache can stay on Codon.

## Production manifest query

Export this SELECT result as a TSV with column names.
The query only reads production data.

```sql
SELECT a.accession AS analysis_accession,
       a.pipeline_version,
       rtrim(a.external_results_dir, '/') || '/' ||
           ltrim(d->>'path', '/') AS rhea_path
FROM analyses_analysis AS a
JOIN analyses_study AS s ON s.accession = a.study_id
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(a.downloads, '[]'::jsonb)) AS d
WHERE a.pipeline_version LIKE 'V6%'
  AND a.assembly_id IS NOT NULL
  AND NOT a.is_private
  AND NOT a.is_suppressed
  AND a.is_ready
  AND NOT s.is_private
  AND NOT s.is_suppressed
  AND d->>'download_group' = 'functional_annotation.rhea_reactions'
  -- Optional study restriction:
  -- AND a.study_id = 'MGYS00000001'
ORDER BY a.accession;
```

## Local PostgreSQL import

Run migration 0062 before importing the compact index.
Production integer IDs can differ from local integer IDs.
The TSV therefore uses stable MGYA accessions.

The current API requires local Analysis records and their related metadata.
The compact TSV supplies molecule sets, not a database metadata snapshot.
Missing MGYAs need a separate metadata import before API searches can return them.
The staging table can retain the exported sets until that import.
Frontend routing remains a separate change.

Run these commands against the local database with psql.
Replace the filename with your local export path.

```sql
\set ON_ERROR_STOP on
CREATE TABLE IF NOT EXISTS rhea_index_staging (
    analysis_accession text PRIMARY KEY,
    chebi_ids integer[] NOT NULL
);
BEGIN;
TRUNCATE rhea_index_staging;
\copy rhea_index_staging FROM '/absolute/local/path/molecule-index.tsv' WITH (FORMAT csv, DELIMITER E'\t', HEADER true)
COMMIT;

-- Resolve missing local analyses before the final INSERT.
SELECT s.analysis_accession
FROM rhea_index_staging AS s
LEFT JOIN analyses_analysis AS a ON a.accession = s.analysis_accession
WHERE a.id IS NULL;

BEGIN;
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM rhea_index_staging AS s
        LEFT JOIN analyses_analysis AS a ON a.accession = s.analysis_accession
        WHERE a.id IS NULL
    ) THEN
        RAISE EXCEPTION 'Import the missing Analysis metadata first';
    END IF;
END $$;

INSERT INTO analyses_analysismoleculeindex (analysis_id, chebi_ids)
SELECT a.id, s.chebi_ids
FROM rhea_index_staging AS s
JOIN analyses_analysis AS a ON a.accession = s.analysis_accession
ON CONFLICT (analysis_id) DO UPDATE SET chebi_ids = EXCLUDED.chebi_ids;
COMMIT;
```

## Initial Prefect flow

Entry point:

```text
workflows/flows/analysis/assembly/flows/initialise_molecule_index.py:initialise_molecule_index
```

The Codon deployment configuration includes `initialise_molecule_index_deployment`.
It has no schedule.
Its required `workdir` parameter must identify shared storage accessible to the flow worker.
The existing `move-data/move_data_deployment` must be registered.

The flow groups public, ready V6 assembly analyses by study.
Readiness means annotation import has completed for that analysis.
The Study model has no separate completion flag.
The flow does not wait for other analyses or optional pipelines in the study.
Private and suppressed studies are excluded.

Each study uses one datamover deployment with an rsync files list.
The source root comes from `EMG_CONFIG.slurm.ftp_results_dir`.
The job uses `EMG_CONFIG.slurm.datamover_partition`.
Only successful copy jobs allow subsequent database imports.
Each analysis then uses the existing atomic Rhea importer.

Parameters:

- `workdir`: required absolute shared directory, visible to the flow worker.
- `study_accession`: optional MGYS accession for a trial import.
- `overwrite`: false by default; true replaces existing index rows.
- `timeout`: maximum wait per study copy, in seconds; default 14400.

For a Slurm worker, supply a path under `shared_filesystem_root_on_slurm`.
For a server worker, supply a path under `shared_filesystem_root_on_server`.
Server paths map to cluster paths through those configured roots.
Both paths must refer to the same shared storage.
The destination contains one directory per MGYS accession.
The copied files remain available for inspection.
Failed or timed-out copies retain their files lists for inspection.

Reruns skip existing index rows, including empty sets.
Failures preserve previous index rows.
Other studies continue after a failure.
The final flow state reports any failed analyses.
No cluster commands run during local development or tests.
