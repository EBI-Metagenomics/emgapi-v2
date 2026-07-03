#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
COMPOSE_FILE="${ROOT_DIR}/docker-compose.yml"
SOURMASH_QUEUE_REPO="${SOURMASH_QUEUE_REPO:-/private/tmp/sourmash-queue}"
export SOURMASH_QUEUE_REPO

compose() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

if [[ ! -f "${SOURMASH_QUEUE_REPO}/tasks.py" ]]; then
  echo "Expected sourmash-queue repo at ${SOURMASH_QUEUE_REPO}" >&2
  exit 1
fi

echo "Using sourmash-queue repo: ${SOURMASH_QUEUE_REPO}"
echo "Starting local sourmash stack..."
compose up -d --build

echo "Preparing sourmash data directories..."
compose exec -T sourmash-queue sh -lc '
  mkdir -p /opt/sourmash/queries /opt/sourmash/results /opt/sourmash/signatures/human-gut-v2-0
  rm -f /opt/sourmash/queries/query.sig
  rm -f /opt/sourmash/results/*.csv /opt/sourmash/results/*.tgz
  rm -f /opt/sourmash/signatures/human-gut-v2-0/*.sig
  rm -f /opt/sourmash/signatures/human-gut-v2-0/genomes_index.sbt.json
  rm -f /opt/sourmash/signatures/human-gut-v2-0/genomes_index.sbt.zip
  rm -rf /opt/sourmash/signatures/human-gut-v2-0/.sbt.genomes_index
'

echo "Building minimal test index..."
compose exec -T sourmash-queue sh -lc '
  sourmash sketch dna \
    --from-file test-data/human-gut-v2-0/all_fasta.txt \
    --outdir /opt/sourmash/signatures/human-gut-v2-0 \
    --name-from-first \
    -p k=31,scaled=1000
'
compose exec -T sourmash-queue sh -lc '
  cd /opt/sourmash/signatures/human-gut-v2-0 &&
  sourmash index -k 31 --scaled 1000 --dna genomes_index *.sig &&
  unzip -o genomes_index.sbt.zip
'

echo "Building query signature..."
compose exec -T sourmash-queue sh -lc '
  sourmash sketch dna \
    test-data/human-gut-v2-0/MGYG000000001.fna \
    -o /opt/sourmash/queries/query.sig \
    -p k=31,scaled=1000
'

echo "Submitting real run_gather task..."
task_id="$(
  compose exec -T sourmash-queue python -c "
from celery import Celery
app = Celery('tasks', broker='redis://redis:6379/0', backend='redis://redis:6379/1')
result = app.send_task('tasks.run_gather', ('query.sig', 'MGYG000000001.fna', 'human-gut-v2-0'))
print(result.id)
"
)"
task_id="${task_id##*$'\n'}"
echo "Task ID: ${task_id}"

echo "Waiting for task completion..."
compose exec -T sourmash-queue python -c "
import sys
import time
from celery import Celery

task_id = '${task_id}'
app = Celery('tasks', broker='redis://redis:6379/0', backend='redis://redis:6379/1')

for _ in range(90):
    result = app.AsyncResult(task_id)
    print(f'STATUS: {result.status}')
    if result.ready():
        if result.successful():
            print(f'RESULT: {result.result}')
            sys.exit(0)
        print(f'FAILURE: {result.result}')
        sys.exit(1)
    time.sleep(1)

print('Timed out waiting for task completion')
sys.exit(1)
"

echo "Checking result CSV..."
compose exec -T sourmash-queue sh -lc "
  test -f /opt/sourmash/results/${task_id}.csv &&
  ls -lh /opt/sourmash/results/${task_id}.csv &&
  head -n 5 /opt/sourmash/results/${task_id}.csv
"

echo "Smoke test passed."
