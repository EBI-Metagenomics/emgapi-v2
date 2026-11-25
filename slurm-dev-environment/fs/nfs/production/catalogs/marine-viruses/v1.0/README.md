# Tiny marine viral catalogue

This fixture reduces every file from the [marine viral catalogue v1](https://ftp.ebi.ac.uk/pub/databases/metagenomics/BlueRemediomics/marine_viral_catalogue_v1/).
The source files were downloaded on 2026-11-18.
The source release does not declare a pipeline version.

## Contents

All **107 source paths** have corresponding fixture files.
[FILES.md](FILES.md) lists their sizes and entry counts.
[source_files.json](source_files.json) records the complete source inventory.
[fixture_manifest.json](fixture_manifest.json) records the fixture checksums and selected sequence identifiers.

The fixture contains:

- Nine real viral fragments, including one prophage.
- One synthetic ASAv6 fragment: `MGYV9999999999`.
- Three real plasmid representatives.
- Nine viral clusters, including one two-member cluster.
- Real AMR hits, host predictions, HMM annotations, and plasmid mobility results.
- Nucleotide sequences, proteins, GFF features, metadata, taxonomy exports, and regenerated indexes.

The initial marine records test a complete two-member cluster and different CheckV quality results.
`MGYV0000111872` adds a real AMR result.
`MGYV0000123871` adds a prophage export.
`MGYV0000125952` adds a spacer-based host prediction.
`MGYV0000098716` adds a lifestyle prediction.
The three plasmids are `MGYV0000022586`, `MGYV0000056305`, and `MGYV0000132735`.
They include AMR and mobility examples.

The synthetic record duplicates the nucleotide sequence of `MGYV0000022715`.
Its source is the development assembly `ERZ857107`.
The development data generator links it to the matching public ASAv6 analysis.
It has no invented protein or functional annotations.
It does not represent a published marine catalogue result.

## Formats and counts

The fixture preserves source headers, columns, sequence identifiers, and relative paths.
Files without matching annotations retain their headers or format scaffolds.
These empty results describe the selected sequences.
They do not contain unrelated annotations from the full release.
The source taxonomy trees and label scaffolds remain intact.
Their count and binary datasets describe the retained taxonomy.

HMM summary counts are recalculated from retained hit records.
Cluster counts and quality ranges are recalculated from retained members.
Protein totals describe the reduced FASTA files.
Mobility summaries describe the retained classification rows.
Krona exports retain their original interactive runtime and reduced taxonomy data.
Sankey HTML exports use reduced data and the source export's Plotly version through its CDN.

**Fragment type and CheckV's provirus flag are different fields.**
The source catalogue has 5,333 prophage fragments but 4,021 CheckV-positive proviruses.
The fixture includes one prophage and two CheckV-positive proviruses.
`MGYV0000123871` is a prophage with a negative CheckV provirus flag.
Catalogue counts and category filters use fragment type.
The API also preserves CheckV's separate flag.

The importer accepts `Source=analysis` or `Source=assembly` for ASAv6 records.
An optional `Source_analysis` column supplies an MGYA link.
An optional `Source_genome` column supplies an MGYG link.
An optional `Sequence_type` column supplies `virus` or `prophage` explicitly.
Without that column, the importer uses the source's `Original_ID` type.
External ENA sources do not require local database links.

## Size

The 107 exports contain **2,673,558 bytes**.
The complete directory also includes manifests and documentation.
The largest records belong to the genuine prophage annotation example.
All source formats and standalone Krona runtimes remain available.
Keeping every export requires some repeated annotation data across files.
The full source download requires approximately 6 GB of local storage.

## Verification

Run the fixture and importer checks from the API repository root:

```bash
docker compose run --entrypoint /bin/bash app -c 'pytest viruses/tests -o addopts="" --no-cov'
```

Expected: ten raw viral sequences, nine imported representatives, three plasmids, and all 107 exports.
No Nextflow module or nf-test snapshot uses this fixture.

## Local import

The normal development data generator imports this fixture.
The task copies all three data directories to the local download service.
The Prefect flow imports metadata and registers curated downloads.
It does not transfer files or start cluster jobs.
Set `ftp_url` to the directory that already serves the release files.
Catalogues remain unpublished unless `is_public` is true.

```python
from workflows.flows.import_viral_catalogue_flow import import_viral_catalogue_flow

import_viral_catalogue_flow(
    results_directory='/nfs/donco/results/marine-viruses/v1.0',
    catalogue_id='v1.0',
    name='Marine Viruses v1.0 (development fixture)',
    version='1.0',
    biome='Marine',
    ftp_url='http://localhost:8080/pub/databases/metagenomics/mgnify_viruses/marine-viruses/v1.0/',
    is_public=True,
)
```

## Database identities and cluster metadata

The importer reads both viral and plasmid metadata tables.
Only cluster representatives become database fragment records.
`ViralFragment` stores each stable accession and nucleotide SHA256.
`ViralCatalogueViralFragment` stores release annotations and the original metadata rows for the complete cluster.
The detail endpoint returns these rows with a link to their source TSV.
Source-analysis and catalogue-genome relations preserve current source visibility checks.

Catalogue identifiers are version IDs such as `v1.0`.
Only one catalogue can be public at a time.
Importing a public release makes previous releases private.
Authenticated Webin administrators can inspect private versions directly.
The latest-release endpoint always selects the public release.

## Website downloads

The fixture retains all 107 source paths.
The API registers 36 labelled downloads from these paths.
The curated groups follow the example pages: coding sequences, metadata, viral taxonomy,
host taxonomy, functional annotation, and protein catalogue.
Additional count files support the website charts.
Raw HMMER outputs, HTML exports, tree files, indexes and intermediate reports remain on FTP.
Indexes for selected FASTA files remain attached to their download records.
