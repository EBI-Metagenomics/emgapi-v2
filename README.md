[![Testing](https://github.com/EBI-Metagenomics/emgapi-v2/actions/workflows/test.yml/badge.svg)](https://github.com/EBI-Metagenomics/emgapi-v2/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/EBI-Metagenomics/emgapi-v2/branch/main/graph/badge.svg?token=27IVW899W8)](https://codecov.io/gh/EBI-Metagenomics/emgapi-v2)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# MGnify DB, API and Automation system
This is the V2 API for [MGnify](https://www.ebi.ac.uk/metagenomics), EBI's metagenomics platform.
It is:
- an architecture for the DB schema/ORM and API (based on Django and Postgres) intended to replace [EMG API v1](https://www.github.com/ebi-metagenomics/emg-api)
- a "production automation system" (based on [Prefect](https://www.prefect.io))  intended to replace [🔒 MI Automation](https://www.github.com/ebi-metagenomics/mi-automation)

## Contents
* [Development setup and running locally](#dev-setup)
* [Developing data models](#django)
* Developing workflows: [basic](#writing-flows) and [detailed](workflows/README.md)
* Developing the slurm integration: [basic](#interacting-with-the-slurm-development-environment) and [detailed](slurm-dev-environment/README.md)
* Deploying to production environment: [summary](#deployment) and [detailed](deployment/ebi-wp-k8s-hl/README.md)

---

## Dev setup
Clone the repo.
`pip install -r requirements-dev.txt` (or just have `pre-commit` installed somehow).

`pre-commit install`.

### Running it
There are three main parts – an API server, a Prefect server, and a Prefect agent.
(There are also database and object stores to run – these can be sqlite/local-fs, but this aims to be a more production-like setup.)
In a real world these would probably live on separate VMs: on HPC, on hosted DBs, and on K8s.
For local development, these are all run in a docker-compose environment.

There is also a docker-compose setup of [Slurm](https://slurm.schedmd.com), so that automation of HPC scheduling can be developed.
This creates a tiny slurm cluster called `donco` (not `codon`).
This is in the `slurm-dev-environment` directory: see [slurm-dev-environment/README.md](slurm-dev-environment/README.md) for more.

There is also an apache web server, as a mock "transfer services area" for serving data files over HTTP.

#### Set up docker-compose
E.g. following [the docker docs](https://docs.docker.com/compose/install/) or using Podman or Colima, as you prefer. In theory all should work.
(There is a docker compose file rooted at `./docker-compose.yaml`,
so later on you can do normal docker/compose things like rebuild a container with `docker-compose build app`.
However, most common tasks are covered by the Taskfile, see below.)

#### Create secrets-local.env
That file is used in development (docker-compose.yml) to export variables into environment.
Currently, that file has mandatory variables: username and password for assembly uploader using [webin-cli](https://ena-docs.readthedocs.io/en/latest/submit/general-guide/webin-cli.html)
```commandline
export EMG_WEBIN__EMG_WEBIN_ACCOUNT="Webin-XXX"
export EMG_WEBIN__EMG_WEBIN_PASSWORD="password"
```

#### The Taskfile
The project has Taskfiles to simplify some common activities. So, [install Task](https://taskfile.dev/installation/).
These are just helpers for local development and deployment.
```shell
task --list-all  # this shows you all of the available commands.
```

#### Make the Django DB and populate it with some data for dev purposes
```shell
task make-dev-data
```
This will have created a Django-managed DB on a dockerized Postgres host, and put some fixtures into it.
It will ask you for a password, which is for a django admin user called `emgdev`. This password can be anything.
(You could also just create/migrate the DB, without the fixture placement, using `task manage -- migrate`.)

#### Set up a couple of common flows for example
```shell
FLOW=realistic_example task deploy-flow  # this "deploys" workflows/flows/realistic_example.py:realistic_example to your local prefect server
# This flow is just a minimal demo to show how the prefect+django integration works.

FILE=workflows/prefect_utils/datamovers.py FLOW=move_data task deploy-flow
# if a flow filename + function name don't match, specify FILE separately.
# This move_data flow needs to be deployed, because it is used by other flows.
```

#### Run everything (the databases, the Django app, the Prefect workflow server, a Prefect work agent, and a small Slurm cluster with associated controllers+dbs.)
```shell
task run
```
> ##### Details
> Be aware this runs 8 containers using ~2GB of RAM. Configure your Podman Machine / Docker Desktop / Colima setup accordingly.
>
> You'll see logs from all the containers.
>
> Depending on your containerisation setup, you may need to tweak the `CPUs=4` line of `slurm-dev-environment/configs/slurm_single_node.conf:45`, e.g. setting it to 1.
> This is related to the number of CPUs on your host machine or on the container VM you're using: e.g. what you set in Docker Desktop.

You can then go to http://127.0.0.1:4200 to see the Prefect dashboard (workflows to be run).
You can also go to http://localhost:8000/api/v2/docs to see the Django app.
The django admin dashboard is at http://localhost:8000/admin (username: `emgdev` if you used `task make-dev-data`).

#### Run a basic Prefect flow
Prefect flows are just Python. There is a hello-world like example in `workflows/flows/simple_example.py`.
It can be run using Python, e.g. inside the `app` container:
```shell
docker-compose exec app python workflows/flows/simple_example.py
```
You'll see that the flow and task decorators break the workflow up into individually executable bits of work.
You can use this kind of approach to debug things.
Meaningful flows, however, are run on separate infrastructure – and that is what the slurm and prefect agent dev environments are for.


#### Register a Prefect flow (new shell)
```shell
FLOW=realistic_example task deploy-flow
```
This "builds" a prefect flow (from the `workflows/flows/` directory, in a file of name `realstic_example` with an `@flow`-decorated method also called `realistic_example`).
(Use `FILE=... FLOW=... task deploy-flow` if the filename doesn't match the method name.)
It also "applies" the "flow deployment", which means the Prefect server knows how to execute it.
It will register it as requiring a worker from the workpool "slurm" to run it.
The Prefect agent in the docker compose setup is labelled as being from this "slurm" pool, so will pick it up.
This agent simulates a worker node on an HPC cluster, e.g. it can submit `nextflow` pipeline executions which can in turn launch slurm jobs.
Note that this is a very minimal development environment... the entire "slurm cluster" is just two docker containers on your computer.

#### Run a flow
Either: open the [Prefect dashboard](http://localhost:4200), or use a POST request on the [MGnify API](http://localhost:8000/api/v2/), or use the prefect CLI via docker compose.

E.g., use the Prefect dashboard to do a "quick run" of the [Realistic Example flow you just deployed](http://localhost:4200/deployments?deployments.nameLike=realistic&page=1) with accession `PRJNA521078`.
This example will:
- make an ENA Study in the database
- suspend itself and wait to be "resumed" in the Prefect dashboard, because it needs to know a "sample limit" from the admin user
- get a list of samples from the ENA API, in an @task
- run a nextflow pipeline for each sample, on slurm, that downloads the read runs

You could also run this newly deployed flow from the command line, using the Prefect CLI. e.g.:
```shell
task prefect -- deployment run "Download a study read-runs/realistic_example_deployment" --param accession=PRJNA521078
```

(Note that you can't run this one in the same way as `simple_example.py`, because `realistic_example.py` does not have a `__main__`).

---

### Code style guide
* Use type hinting: `def my_func(param: List[str]) -> int:`
* Prefer to use `pathlib` instead of `os.path`, e.g. for joining parts: `Path("/nfs/my/dir") / "subdir" / "file.txt"`
* Config parameters (like the URL for ENA etc.) should use structured [Pydantic Settings](https://docs.pydantic.dev/latest/concepts/pydantic_settings/). See `emgapiv2/settings.py:EMG_CONFIG`.
* `EMG_CONFIG` should [always be imported via `django.conf.settings`](https://docs.djangoproject.com/en/5.1/topics/settings/#using-settings-in-python-code): e.g. `from django.conf import settings; EMG_CONFIG = settings.EMG_CONFIG`
  * In flows, you can fetch it and ALSO ensure Django is properly activated by using `from activate_django_first import EMG_CONFIG`. This should be near the top of every Prefect flow, ABOVE any model imports.
* When you have a list of acceptable options for something, use `Enum`s or `TextChoices` (a kind of enum for Django db fields): `class AssemblyStatuses(FutureStrEnum):...` or `DjangoChoicesCompatibleStrEnum` if it is used both as a general `Enum` and as `choice=` parameter of a Django field.
* Use Django/postgres JSONFields liberally (they can save a load of complicated JOINs)
* Apply a schema to JSONFields, using Enums, default dicts, custom pydantic types... see `WithDownloadsModel` for an example
* Use class mixins and Django abstract models liberally, to add shared/similar functionality to multiple models
* API list endpoints should not perform many/any JOINs. Prefer to have less information on the endpoint than introduce JOINs (it can be separately indexed for search)
* API detail endpoints may perform JOINs
* API action endpoints (e.g. `/analyses/MGYA1/taxonomies`) should be used where a very large dataset (`taxonomies`) is to be returned. This means `taxonomies` is not needed (so can be deferred) on the main analysis detail endpoint.
* Use a variable for the labels of JSON/dicts: `STATUS = "status"; my_dict = {STATUS: get_status_of_run()}`
* Use ReST style docstrings for functions: `:param sample_accession: The sample to be analysed`

## Developing models and flows

### Django
There are two real Django apps here:
* `ena`, for models that mirror objects in ENA: studies, samples, etc.
* `analyses`, for models associated with MGnify analysis production work (MGYS, MGYA etc).
* TODO: other models like genomes could live in separate apps.

There is one fake Django app `workflows`, which is used to tie Prefect (the workflow scheduler) into Django.
This is bidi: it creates a `manage.py prefectcli` command to run Prefect, and it allows Prefect tasks to use instantiated Django.

The API is implemented with `ninja` (`emgapiv2/api.py`), and uses Open API spec with Swagger.

### Writing flows
See [the workflows/README.md](workflows/README.md) for details. In short: add Python/Prefect code to a file in `workflows/flows/` and then `FLOW=my_flow task deploy-flow`.

### Testing
The project uses the [pytest](https://docs.pytest.org) framework.
[Prefect has some helpers](https://docs.prefect.io/latest/guides/testing/) for testing.
We also use [Pytest-django](https://pytest-django.readthedocs.io/en/latest/) to help with Django testing.

Testing libraries are in `requirements-dev.txt`. These are installed in the docker compose `app` container. So:

```shell
task test
# ...will run everything. Or for a subset, use pytest arguments after -- e.g.:
task test -- -k study
```


### Interacting with the Slurm development environment
See [the slurm-dev-environment/README.md](slurm-dev-environment/README.md) for details. In short: `task slurm` and you're on a slurm node
of the containerised slurm "cluster".

---

## Deployment
The `deployment/` folder has deployment configs for different environments.
Each should have its own `Taskfile`, included in the main `Taskfile`.
E.g. see [the EBI WP K8s HL deployment README](deployment/ebi-wp-k8s-hl/README.md).
Run e.g. `task ebi-wp-k8s-hl:update-api` to build/push/restart the EMG API service in that deployment (**requires some secrets setup**).

Run e.g. `FLOW=assembly_study task ebi-wp-k8s-hl:deploy-flow` to deploy a new flow to this production environment.

Note that the prefect workers *ALSO* need to have your new flow code, which is currently deployed separately.
For EBI-WP-K8s-HL, there is a Jenkins job to deploy those workers to Codon.


## TODO
* DB Schema parity with EMG DB (v1) and EMG Backlog
* Job cleanup flows
* Legacy data importers
