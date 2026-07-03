# Local Sourmash Stack

This helper stack runs the legacy `sourmash-queue` worker locally with:

- Redis broker/backend on `localhost:6380`
- Celery worker (`tasks.run_gather`)
- Flower dashboard on [http://127.0.0.1:5555](http://127.0.0.1:5555)

By default the compose file expects the `sourmash-queue` repo at `/private/tmp/sourmash-queue`.
Override that by exporting `SOURMASH_QUEUE_REPO=/path/to/sourmash-queue`.

## Usage

Bring the stack up:

```bash
docker compose -f tools/sourmash-local/docker-compose.yml up -d --build
```

Run the end-to-end smoke test:

```bash
tools/sourmash-local/smoke_test.sh
```

Tear the stack down:

```bash
docker compose -f tools/sourmash-local/docker-compose.yml down
```

## What the smoke test does

1. Starts Redis, the Celery worker, and Flower.
2. Builds a tiny test index from `sourmash-queue/test-data/human-gut-v2-0/`.
3. Builds a query signature from `MGYG000000001.fna`.
4. Submits a real `tasks.run_gather` task through Celery.
5. Waits for the task to complete and checks that a CSV result was written.
