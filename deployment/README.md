# Deployment configuration

Production and shared-environment deployment configuration is intentionally not
kept in this application repository.

Kubernetes manifests, migration jobs, production Prefect deployment declarations,
cluster-specific Taskfiles, worker supervisor scripts, ingress rules, NFS paths,
namespaces, kubeconfig references, pull-secret references, and secret runbooks
belong in the private infrastructure repository.

This repository should contain only application code, local development tooling,
tests, and container build inputs.


## Branchwater background searches

Branchwater uses the existing Django Tasks database backend and the dedicated
`branchwater` queue. Run a worker with `python manage.py db_worker --queue-name branchwater`
alongside the API. Existing Sourmash workers can continue consuming `default`.
A worker restricted to `default` does not process Branchwater jobs. The local
Compose `app` and `all` profiles therefore include a dedicated
`branchwater-worker`; the existing `app-worker` remains restricted to `default`.
The existing Django Tasks database migrations are sufficient; no Branchwater
model migration is required.

The API and worker must use the same database and share a writable filesystem for
`EMG_BRANCHWATER__QUERIES_PATH` and `EMG_BRANCHWATER__RESULTS_PATH`. Local defaults
are `/tmp/branchwater/queries` and `/tmp/branchwater/results`; container deployments
must mount the same persistent volume at both configured paths with compatible
file ownership. Local Compose mounts the `branchwaterjobs` named volume at
`/var/lib/branchwater` in both services. The worker needs access to the Rust index
server and metadata DuckDB file; the API also contacts Rust for its health endpoint.
Configure the worker search timeout with
`EMG_BRANCHWATER__SEARCH_TIMEOUT_SECONDS` (default: 300 seconds). The API still
needs the configured signature root for MAG submissions.

Both POST routes return HTTP 202 with `data.job_id`, `status`, `status_url` and
`results_url`. Poll `status_url` until `SUCCESS`, `NO_RESULTS` or `FAILED`, then
fetch `results_url` for the existing enriched JSON array on success. Pending or
failed result requests return HTTP 409. Unknown or unrelated task IDs return 404.
Results and query files should be retained together with the task records for the
service's chosen retention period; cleanup is an infrastructure responsibility.

An enqueue error can have an ambiguous outcome, so its staged query is preserved
instead of risking deletion of input already accepted by the backend. Include
these orphaned submission directories in the retention policy.

The current Django Tasks database worker does not automatically reclaim jobs
left `RUNNING` after an interrupted process. Avoid autoreload or forced worker
restarts while a search is running. If a development worker is interrupted,
resubmit the search; a persistent `RUNNING` state does not prove that a worker is
still processing it. Automatic recovery is not implemented by these endpoints.
