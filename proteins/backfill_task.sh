#!/bin/bash
# One task of a backfill array: accessions line OFFSET + SLURM_ARRAY_TASK_ID of TASKS.
# Usage: backfill_task.sh TASKS OFFSET SIF, with PROTEINDB_DSN in the environment.
set -euo pipefail

IFS=$'\t' read -r ACC VER FAA GFF CONTIGS OUT < <(sed -n "$(($2 + SLURM_ARRAY_TASK_ID))p" "$1")
mkdir -p "$(dirname "$OUT")"
singularity exec -B "$(dirname "$FAA")" -B "$(dirname "$CONTIGS")" -B "$(dirname "$OUT")" "$3" \
    mgyp-accession --assembly "$ACC" --pipeline-version "$VER" \
    --faa "$FAA" --gff "$GFF" --contigs "$CONTIGS" --out "$OUT"
