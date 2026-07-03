# Sourmash Search via Django Tasks

## Goal

Move sourmash search fully into `emgapi-v2`, replace the current Celery + external worker coupling with:

- Django 5.2-compatible `django-tasks` backport
- `django-tasks-db` as the queue/backend
- one API image with two runtime roles:
  - web/API
  - `manage.py db_worker`

This document is the repo-level design for the first implementation pass.

## Scope for the first pass

Included:

- replace Celery enqueueing and result lookup for `/genomes-search/gather/`
- add first-class DB models for search jobs and active sourmash indexes
- keep the worker logic in-house
- keep result enrichment in the API layer

Not included:

- Branchwater migration
- raw FASTA query sketching inside the API
- generalized multi-backend search orchestration

## Why stay on Django 5.2 for now

We should use the Django 6 Tasks API shape, but implement it with the Django 5.2 backport packages first:

- `django-tasks`
- `django-tasks-db`

That keeps this change focused on sourmash architecture rather than folding in a broader Django 6 framework upgrade.

To make a later Django 6 upgrade easy, production code should not import directly from `django_tasks` all over the tree. Instead, add a tiny adapter module:

```python
# emgapiv2/tasking.py
from django_tasks import TaskContext, default_task_backend, task
```

Later, on Django 6, that module can change to:

```python
from django.tasks import TaskContext, default_task_backend, task
```

## Current problems this design fixes

The current gather implementation is Celery-shaped:

- it builds a Celery app in the API process
- it fans out with a Celery `group`
- it restores status via Celery group results
- it assumes the worker has its own authoritative catalogue map

That shape lives in:

- [emgapiv2/api/genome_search_gather.py](/Users/mahfouz/Code/mgnify-web/emgapi-v2/emgapiv2/api/genome_search_gather.py)
- [/private/tmp/sourmash-queue/tasks.py](/private/tmp/sourmash-queue/tasks.py)
- [/private/tmp/sourmash-queue/settings-local.py](/private/tmp/sourmash-queue/settings-local.py)

The main architectural problem is state drift:

- the API validates against `GenomeCatalogue`
- the worker validates against its own `MAG_CATALOGS`

The replacement design makes the DB the only source of truth for:

- which catalogues are searchable
- which exact sourmash artifact should be used
- which child searches belong to a request
- what status each child search is in

## Proposed runtime architecture

```mermaid
flowchart LR
    client["Client"]
    api["API container\nGunicorn / Django / Ninja"]
    worker["Worker container\npython manage.py db_worker"]
    db["Postgres\nMGnify DB + task backend"]
    nfs["Mounted sourmash artifact store"]

    client -->|POST /genomes-search/gather/| api
    api -->|validate indexes + create job rows| db
    api -->|stage query signature| nfs
    api -->|enqueue child tasks| db
    api -->|202 + job id| client

    worker -->|claim task| db
    worker -->|load job/item/index rows| db
    worker -->|read query + index artifact| nfs
    worker -->|write item result + status| db
    worker -->|write raw CSV if needed| nfs

    client -->|GET /genomes-search/status/{job_id}/| api
    api -->|aggregate item state| db
    api -->|enrich top hits with MGnify metadata| db
    api -->|status/result payload| client
```

## Core design principle

Do not use Django `TaskResult` as the business object exposed by the API.

Use Django tasks only as the execution mechanism.

The API should expose and persist its own domain objects:

- `GenomeSearchIndex`
- `SourmashSearchJob`
- `SourmashSearchJobItem`

This is the replacement for Celery `group` semantics.

## Proposed model shape

### 1. `GenomeSearchIndex`

App location:

- `genomes/models/search_index.py`

Purpose:

- register the exact searchable sourmash artifact for a catalogue
- let the API validate against the same index metadata the worker uses
- support multiple backends later

Suggested model:

```python
class GenomeSearchIndex(models.Model):
    class Backend(models.TextChoices):
        SOURMASH = "sourmash", "Sourmash"
        BRANCHWATER = "branchwater", "Branchwater"

    class Status(models.TextChoices):
        BUILDING = "BUILDING", "Building"
        ACTIVE = "ACTIVE", "Active"
        RETIRED = "RETIRED", "Retired"
        FAILED = "FAILED", "Failed"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    catalogue = models.ForeignKey(
        GenomeCatalogue,
        on_delete=models.CASCADE,
        related_name="search_indexes",
    )
    backend = models.CharField(max_length=32, choices=Backend.choices)
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.BUILDING)
    is_active = models.BooleanField(default=False)

    ksize = models.PositiveSmallIntegerField()
    moltype = models.CharField(max_length=16, default="DNA")
    scaled = models.PositiveIntegerField(null=True, blank=True)

    artifact_path = models.CharField(max_length=500)
    manifest_path = models.CharField(max_length=500, blank=True)
    genome_count = models.PositiveIntegerField(default=0)
    checksum = models.CharField(max_length=128, blank=True)

    built_at = models.DateTimeField()
    activated_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
```

Notes:

- worker code should resolve by `GenomeSearchIndex`, not by hardcoded settings dict
- the first pass should only use `backend="sourmash"`
- add a conditional uniqueness constraint so only one active sourmash index exists per `(catalogue, backend, ksize, moltype)`

### 2. `SourmashSearchJob`

App location:

- `genomes/models/sourmash_search_job.py`

Purpose:

- represent one API submission
- provide a stable business ID for polling
- aggregate child search items

Suggested model:

```python
class SourmashSearchJob(models.Model):
    class Status(models.TextChoices):
        RECEIVED = "RECEIVED", "Received"
        QUEUED = "QUEUED", "Queued"
        RUNNING = "RUNNING", "Running"
        SUCCESS = "SUCCESS", "Success"
        PARTIAL_SUCCESS = "PARTIAL_SUCCESS", "Partial success"
        FAILED = "FAILED", "Failed"
        NO_RESULTS = "NO_RESULTS", "No results"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    status = models.CharField(max_length=32, choices=Status.choices, default=Status.RECEIVED)

    query_original_name = models.CharField(max_length=255)
    query_staged_path = models.CharField(max_length=500)
    request_payload = models.JSONField(default=dict)

    submitted_by = models.CharField(max_length=255, blank=True)
    submitted_at = models.DateTimeField(auto_now_add=True)
    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)

    raw_results_archive_path = models.CharField(max_length=500, blank=True)
    error_summary = models.TextField(blank=True)
```

Notes:

- `submitted_by` can be blank in the first pass if this endpoint stays anonymous
- `request_payload` is useful for debugging and later replay

### 3. `SourmashSearchJobItem`

App location:

- `genomes/models/sourmash_search_job.py`

Purpose:

- represent one `(job, active index)` execution unit
- let one request fan out across multiple catalogues without Celery groups

Suggested model:

```python
class SourmashSearchJobItem(models.Model):
    class Status(models.TextChoices):
        RECEIVED = "RECEIVED", "Received"
        QUEUED = "QUEUED", "Queued"
        RUNNING = "RUNNING", "Running"
        SUCCESS = "SUCCESS", "Success"
        FAILED = "FAILED", "Failed"
        NO_RESULTS = "NO_RESULTS", "No results"

    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    job = models.ForeignKey(
        SourmashSearchJob,
        on_delete=models.CASCADE,
        related_name="items",
    )
    search_index = models.ForeignKey(
        GenomeSearchIndex,
        on_delete=models.PROTECT,
        related_name="job_items",
    )
    status = models.CharField(max_length=16, choices=Status.choices, default=Status.RECEIVED)

    task_result_id = models.CharField(max_length=64, blank=True)
    raw_csv_path = models.CharField(max_length=500, blank=True)
    result_summary = models.JSONField(default=dict)
    error_message = models.TextField(blank=True)

    started_at = models.DateTimeField(null=True, blank=True)
    finished_at = models.DateTimeField(null=True, blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
```

Notes:

- `task_result_id` is useful but not authoritative
- the authoritative status for the API should be the item row itself
- `search_index` uses `PROTECT` so historical jobs remain tied to the exact artifact record

## Proposed task shape

App location:

- `genomes/tasks.py`

Suggested task:

```python
@task(takes_context=True, queue_name="default")
def run_sourmash_gather_item(context: TaskContext, job_item_id: str) -> dict:
    ...
```

Why pass only `job_item_id`:

- task args must remain JSON-serializable
- a single ID is the smallest stable payload
- the worker can fetch the exact `search_index`, `catalogue`, and query path from the DB
- it avoids duplicating business state across the queue payload and DB rows

### Task responsibilities

The task should:

- load `SourmashSearchJobItem` with related `job` and `search_index`
- mark the item `RUNNING`
- load the staged query signature from `job.query_staged_path`
- load the sourmash index from `search_index.artifact_path`
- run gather/search logic
- write optional raw CSV to disk
- persist `result_summary`, `raw_csv_path`, `status`, and timestamps back onto the item row
- return a small JSON-safe summary

The task should not:

- determine which catalogues are valid
- enrich hits with full MGnify ORM metadata for the final public response
- maintain a separate catalogue mapping in Python settings

### Suggested task return value

```python
{
    "job_item_id": "<uuid>",
    "status": "SUCCESS",
    "catalogue_id": "human-gut-v2-0-2",
    "search_index_id": "<uuid>",
    "match_count": 7,
    "top_hit": {
        "accession": "MGYG000000001",
        "overlap": "3.2 Mbp",
        "p_query": "100.0%",
        "p_match": "100.0%",
    },
    "raw_csv_path": "/app/data/sourmash/results/<job>/<item>.csv",
}
```

This return value is intentionally small.

The API should read from `SourmashSearchJobItem` rows, not from task return values alone.

## Query staging

First pass recommendation:

- keep the request contract close to the current endpoint
- accept uploaded sourmash signature JSON
- store it once on disk
- reuse that same staged file across all child items

Suggested path layout:

- query: `<queries_root>/<job_id>/query.sig`
- item CSV: `<results_root>/<job_id>/<job_item_id>.csv`
- tarball: `<results_root>/<job_id>/<job_id>.tgz`

This avoids storing large blobs in Postgres while keeping all paths deterministic.

## API shape

Keep the public route:

- `POST /genomes-search/gather/`

But change the implementation from Celery fan-out to DB-backed job creation + Django task enqueueing.

### POST `/genomes-search/gather/`

First-pass flow:

1. validate `mag_catalogues`
2. resolve active `GenomeSearchIndex` rows for:
   - `backend="sourmash"`
   - requested catalogue IDs
   - default `ksize=31`
3. reject any requested catalogue that has no active sourmash index
4. validate uploaded signature
5. stage the query file
6. create one `SourmashSearchJob`
7. create one `SourmashSearchJobItem` per resolved active index
8. enqueue one Django task per item inside `transaction.on_commit(...)`
9. return `202 Accepted`

Suggested response:

```json
{
  "data": {
    "job_id": "<uuid>",
    "status": "QUEUED",
    "signatures_received": ["query.sig"],
    "requested_catalogues": ["human-gut-v2-0-2", "marine-v2-0"],
    "status_url": "/genomes-search/status/<job_id>/"
  }
}
```

### GET `/genomes-search/status/{job_id}/`

This endpoint should aggregate item rows.

Suggested job-level status rules:

- all `RECEIVED` / `QUEUED` -> `QUEUED`
- any `RUNNING` -> `RUNNING`
- all `NO_RESULTS` -> `NO_RESULTS`
- all `SUCCESS` -> `SUCCESS`
- any `SUCCESS` plus any `FAILED` / `NO_RESULTS` -> `PARTIAL_SUCCESS`
- all `FAILED` -> `FAILED`

Suggested per-item status payload:

```json
{
  "job_item_id": "<uuid>",
  "catalogue_id": "marine-v2-0",
  "status": "SUCCESS",
  "match_count": 4,
  "results_url": "/genomes-search/results/<job_item_id>/"
}
```

### GET `/genomes-search/results/{job_id}/`

Behavior:

- if a single CSV exists for the supplied ID, return CSV
- if the ID is a parent job, lazily build a tarball from child CSV files and return it

This preserves the current download affordance without requiring CSV contents to live in the task backend.

## Enqueueing pattern

Use `transaction.on_commit(...)` so the worker never sees a task before the job rows are committed.

Suggested pattern:

```python
with transaction.atomic():
    job = SourmashSearchJob.objects.create(...)
    items = [
        SourmashSearchJobItem.objects.create(job=job, search_index=index)
        for index in search_indexes
    ]

    def enqueue_items(item_ids: list[str]) -> None:
        for item_id in item_ids:
            task_result = run_sourmash_gather_item.enqueue(job_item_id=str(item_id))
            SourmashSearchJobItem.objects.filter(id=item_id).update(
                status=SourmashSearchJobItem.Status.QUEUED,
                task_result_id=task_result.id,
            )

    transaction.on_commit(lambda: enqueue_items([str(item.id) for item in items]))
```

## Why this replaces Celery `group`

Current Celery code depends on:

- `group(...)`
- `result.id`
- `result.results`
- `GroupResult.restore(...)`

Django Tasks is intentionally simpler.

Instead of emulating Celery groups, we should model the fan-out ourselves:

- parent job row = the old group ID
- child item rows = the old child tasks
- aggregated status endpoint = the old `GroupResult.restore(...)`

That makes the API behavior explicit and removes coupling to a queue framework feature set.

## Settings shape

We already have a `SourmashConfig` in [emgapiv2/config.py](/Users/mahfouz/Code/mgnify-web/emgapi-v2/emgapiv2/config.py).

Recommended first-pass fields:

```python
class SourmashConfig(BaseModel):
    queries_path: str = "/tmp/sourmash/queries"
    results_path: str = "/tmp/sourmash/results"
    index_root: str = "/app/data/sourmash/indexes"
    default_ksize: int = 31
    result_retention_days: int = 30
```

And in settings:

```python
INSTALLED_APPS += [
    "django_tasks",
    "django_tasks_db",
]

TASKS = {
    "default": {
        "BACKEND": "django_tasks_db.DatabaseBackend",
        "QUEUES": ["default"],
    }
}
```

For tests:

```python
TASKS = {
    "default": {
        "BACKEND": "django_tasks.backends.immediate.ImmediateBackend",
        "QUEUES": ["default"],
    }
}
```

That aligns with the existing draft test file:

- [emgapiv2/test_task_settings.py](/Users/mahfouz/Code/mgnify-web/emgapi-v2/emgapiv2/test_task_settings.py)

## Proposed file layout

### New or rewritten modules

- `emgapiv2/tasking.py`
- `genomes/models/search_index.py`
- `genomes/models/sourmash_search_job.py`
- `genomes/tasks.py`
- `genomes/sourmash.py`
- `emgapiv2/api/genome_search_gather.py`

### Supporting updates

- `genomes/admin/`
- `genomes/migrations/`
- `emgapiv2/settings.py`
- `emgapiv2/settings_test.py`
- `requirements.txt`
- `docker-compose.yaml`

## Migration sequence

### Step 1

Enable `django-tasks` and `django-tasks-db` in settings and tests.

### Step 2

Add `GenomeSearchIndex`, `SourmashSearchJob`, and `SourmashSearchJobItem`.

### Step 3

Port the worker logic from the external `sourmash-queue` repo into `genomes/tasks.py` plus helper functions.

### Step 4

Rewrite `/genomes-search/gather/` to:

- resolve active indexes
- create job/item rows
- enqueue Django tasks

### Step 5

Rewrite status and results endpoints to read from our own models instead of Celery results.

### Step 6

Add a local worker service to `docker-compose.yaml`:

- same image as `app`
- command: `python manage.py db_worker`

### Step 7

Retire Celery/Redis-specific sourmash code after parity is proven.

## Open questions to settle before patching

1. Should the first pass continue to accept only sourmash signature uploads, or also raw FASTA?
2. Do we want to keep the current `/genomes-search/gather/` route name exactly, or introduce `/genome-search/sourmash/` and deprecate later?
3. Should an item with `NO_RESULTS` count toward overall `SUCCESS`, or should it produce `PARTIAL_SUCCESS` when mixed with real hits?
4. Do we want one active sourmash index per catalogue and k-size, or allow multiple active revisions behind a feature flag?

## Recommended decision

For the first implementation pass:

- keep the route
- keep uploaded sourmash signatures
- default to `ksize=31`
- implement explicit DB-backed job aggregation
- use `django-tasks` + `django-tasks-db`
- defer Django 6 upgrade until after the sourmash queue refactor lands cleanly
