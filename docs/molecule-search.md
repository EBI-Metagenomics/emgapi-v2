# Molecule search

Molecule search uses IDSM's public ChEBI similarity endpoint.
It does not require RDKit, ClassyFire, or a separate search worker.

## Setup

Run `python manage.py migrate` before serving the new endpoints.
Migration 0062 converts existing prototype rows into molecule sets.
It then removes the protein-level prototype table.
The API worker requires outbound HTTPS access to ChEBI and IDSM.

## Storage

`analyses_analysismoleculeindex` contains one row per assembly analysis.
The analysis foreign key is the primary key.
`chebi_ids` contains sorted, unique integer ChEBI identifiers without the prefix.
A GIN index supports array-overlap queries.
Separate analysis rows preserve pipeline versions and visibility rules.
An empty array records a successful import with no molecules.
Protein identifiers, reaction identifiers, and evidence labels remain in the source downloads.

## Endpoints

- `GET /molecule-search/compounds?term=caffeine`: ChEBI candidates with names, identifiers, and SMILES.
- `POST /molecule-search/`: chemical matching and public assembly analyses.

Example request:

```json
{"smiles": "CCO", "threshold": 0.8, "after": 0}
```

The response contains `molecules`, `truncated`, `items`, and `next_cursor`.
Each item contains `analysis`, `assembly`, and its matching `molecules` with similarity scores.
Molecules within each item follow descending similarity order.
Send `next_cursor` as `after` for the next page of 50 analyses.
Pages follow analysis ID order, rather than similarity order.
The chemistry lookup includes at most 100 compounds.
`truncated` indicates additional chemical matches.
Only public, ready, unsuppressed assembly analyses appear in results.
Provider failures return HTTP 502, rather than empty results.

## Imports

See [Initial molecule index](initial-molecule-index.md) for datamover copies and a standalone production-data export.

The ASA results importer calls `import_rhea_annotations` after importing each analysis's downloads.
The task reads the seven-column `proteins2rhea.tsv.gz` output.
It accepts the current header and the older headerless format.
It collects distinct ChEBI identifiers from the reaction-participant column.
A complete parse precedes the atomic database update.
Malformed files preserve the previous molecule set.
Empty files store an empty array.

The Prefect backfill entry point is:

```text
workflows/flows/analysis/assembly/flows/backfill_rhea.py:backfill_rhea
```

Parameters:

- `study_accession`: optional MGnify study restriction.
- `overwrite`: defaults to false; true replaces existing molecule sets.

The backfill reads existing public V6 assembly download records.
It downloads one compressed Rhea table at a time.
It skips analyses without Rhea downloads.
It logs import failures, continues other analyses, and fails after the pass if necessary.
Existing molecule-index rows, including empty arrays, are skipped unless overwrite is true.

## Contig search

The frontend's existing IndexedDB search retains arbitrary GFF attribute text and feature coordinates.
It can search ChEBI identifiers when the merged GFF includes those attributes.
The current ASA CREATE_GFF_SUMMARY process does not pass Rhea annotations into the GFF builder.
Production GFF checks confirmed missing ChEBI attributes in the inspected files.
Global molecule hits therefore do not guarantee ChEBI matches in the current contig browser.
A separate pipeline change must add Rhea/ChEBI attributes to the merged GFF for that workflow.
Older published GFFs would also need regeneration or another enrichment route.

These are predicted reaction participants, not measured metabolites or demonstrated enzyme activities.

The frontend lists the returned IDSM molecule identifiers and similarity scores.
Matched ChEBI links open `/analyses/{accession}/functional?type=rhea&chebi=CHEBI%3A{id}`.
The Rhea TSV search dialog opens with that exact ChEBI identifier prefilled.
This drill-down uses the Rhea download and does not require GFF enrichment.
