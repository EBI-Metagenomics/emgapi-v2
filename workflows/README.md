# Writing Prefect flows for MGnify

## Tasks and flows
[Flows](https://docs.prefect.io/latest/concepts/flows/) and [Tasks](https://docs.prefect.io/latest/concepts/tasks/) are the two building blocks of Prefect workflows.

They are both written as Python functions decorated with `@flow` or `@task`.

Tasks are single steps that need to happen in a workflow. Tasks must be called from flows.

Flows are sequences of tasks, other flows, and arbitrary code.
A flow is the entry point to something being run by Prefect.
A flow can call other flows as subflows.
Flows and tasks can persist results (to storage) which means that if a parent flow needs to be rerun (e.g. because it is paused and resumed), the previously run tasks and flows will not re-execute.

This point is quite important: since tasks and flows look like normal Python functions, you need to remember that a non-decorated function *will* run every time a flow is resumed.

This doesn't usually matter, since in a standard Prefect setup the only time a flow would re-run is if it is retrying on failure.

However, when mixed with long-running HPC jobs (e.g. multi-day nextflow pipelines), our implementation of Prefect will regularly re-run flows.

The key is to make sure that expensive work, that should only be done once, is an at @task with a cache key that means it can be reused.
See the "realistic example" flow for how to achieve this.

## Long-running pipelines
Prefect assumes it will control a pipeline throughout execution; i.e. there is a process running on a prefect agent at all times a flow is running.

Consider a typical MGnify flow, though:
* take a study input
* fetch some data from ENA API
* insert something into the EMG database
* submit a cluster job, to run a nextflow pipeline. It may pend for 2 days and run for 4.
* run another pipeline if certain conditions are met
* submit a datamove job
* insert some other things into a database

There are steps here where Prefect may crash but we would like work to continue – because Prefect is only monitoring work that is really being orchestrated by nextflow and slurm

For this reason, we need to make sure that Prefect flows can be resumed and re-attach themselves to the pipeline runs.

We do this by:
1. submitting jobs to the Slurm cluster;
2. checking the state of the Slurm jobs;
3. making sure that identical job submissions do not start new slurm jobs, but rather return the previously started ones;
4. also having a "pre-cluster delay" to make sure we don't flood the HPC cluster with jobs.

There are helpers for this process in `prefect_utils/slurm_flow.py`.

Essentially, look at `flows/realistic_example.py` for the details.


## Batch analyses
For assembly analyses workflows, we process them in batches and we persist the batch state to the database.
This is particularly important for assembly analyses where we need to run multiple sequential pipelines (ASA → VIRify → MAP) across many assemblies.

The `AssemblyAnalysisBatch` model groups analyses from a single study into manageable chunks.
Each batch tracks the status of three pipelines (ASA, VIRify, and MAP) independently.
The through-table `AssemblyAnalysisBatchAnalysis` lets us track per-analysis pipeline states within the batch, and allows selective disabling of specific analyses.

Batches are created via the manager method `AssemblyAnalysisBatch.objects.get_or_create_batches_for_study()`, which handles:
* Filtering analyses that are ready for processing
* Chunking into configurable batch sizes
* Creating workspace directories for pipeline outputs
* Managing samplesheets for each pipeline stage

This batching approach is experimental and may evolve as we learn more about optimal batch sizes, error handling patterns, and resource utilization on the cluster.

For implementation details, see `workflows/models.py` (batch models) and `workflows/flows/analysis/assembly/` (batch flow orchestration).

## Triggering flows from models
A common pattern in production is likely to be triggering a workflow (a Prefect flow) when an object in the EMG
database changes.
For example, when a Study is created, run some prefect flow to fetch it from ENA, and assemble it's reads.
The pattern for this is to use [signals](https://docs.djangoproject.com/en/5.0/topics/signals/).
Signals (e.g., `post_save`) are events/hooks/triggers that call some function e.g. before or after a model is saved.
(Typically `post_save` is preferable, as it limits the amount of possibly failing work done before commiting the thing to the database.)
To keep things clean, flow-invoking-model-hooks should be registered in the `workflows/signals.py` file.

For example:
```python
@receiver(post_save, sender=analyses.models.AssemblyAnalysisRequest)
def on_assembly_analysis_saved(sender, instance: analyses.models.AssemblyAnalysisRequest, created, **kwargs):
    if not created: return
    flowrun = async_to_sync(run_deployment)(
        "Assemble and analyse a study/assembly_analysis_request_deployment", # <-- naming here is NOT arbitrary
        timeout=0,
        parameters={
            "accession": instance.requested_study,
            "request_id": instance.id,
        },
    )
    instance.request_metadata['flow_run_id'] = str(flowrun.id) # <-- potentially useful to save this for admin purposes
    instance.save()
```
