# Context for JetBrains Junie LLM

## What this is
A Django application for a metagenomics service called MGnify.
Also, an instance of Prefect workflow orchestration system.
The Prefect flows run on a Slurm HPC cluster, usually running nextflow pipelines.
The inputs and outputs of the pipelines are in the Django DB, which is publicly available via a Ninja API.

## Running commands
There is a Taskfile.yaml at the project top-level. Check that first.

## Development setup
There is a docker-compose.yaml at the project top-level. It runs the application components and databases etc.
It also imports more docker-compose services from the slurm-dev-environment dir, which is a dockerised impersonation
of the Slurm HPC setup - e.g. a slurm controller, worker, and file systems.
This is ONLY for local dev - experimenting, really.
In CI/CD, everything is mocked. In production, we use kubernetes and a real slurm cluster.
See the deployment/ dir for clues on the production setup.

## Running the project in dev mode
`task run`

## Running the unit test suite
You may run the tests with the command:
`task test`
(This will run in docker compose.)

### Running a single test (e.g. a new one)
For example for some new test called `test_new_feature_test_1`, use the `testk` task which is basically `pytest -k`:
`task testk -- test_new_feature`

### Avoid checking in secrets
Note that the codebase will be scanned by `talisman` in pre-commit. Details in top-level `.talismanrc`.

## Python style
Black. Will be checked by pre-commit anyway.

## Prefect flows
Everything to do with running pipelines and flows etc is under `workflows/` dir.

## Avoiding Django "not yet initialized" errors
In scripts like prefect flows, include `from activate_django_first import EMG_CONFIG` BEFORE any django model imports.
This makes sure Django is activated when prefect runs a flow in a new process, before model imports are attempted.
