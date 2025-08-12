# Docker-composed Slurm cluster for local development

This directory is a configuration of a minimal but functioning Slurm cluster, running on Docker Compose,
intended for development and testing of Slurm-integrated code, e.g. job submission/running/polling via [PySlurm](https://github.com/PySlurm/pyslurm).

It also includes helper type config for running [Prefect](https://prefect.io) flows on the Slurm cluster.

## Running it
From the parent directory, `docker compose --profile slurm up` will build and start the Slurm cluster.
This `include`s the `docker-compose.yaml` file in this `slurm` dir.

## What you get
It starts:
* `slurm_db`: a Maria DB host for the `slurm` database. Port `3306` is mapped to your host machine, so you can inspect the Slurm Job DB at `mariadb://slurm:slurm@localhost:3306/slurm` (see e.g. `donco_job_table`).
* `slurm_node`: a container running the slurm database daemon, controller, worker node, and the rest api (see below).
  * [Slurm database daemon](https://slurm.schedmd.com/slurmdbd.html). This is the interface between Slurm and the above DB.
  * [Slurm controller / central manager daemon](https://slurm.schedmd.com/slurmctld.html). This is the node workers poll for jobs.
  * [Slurm compute node daemon](https://slurm.schedmd.com/slurmd.html). Acts as a node that executes jobs in the queue.
  * [Slurm rest api daemon](https://slurm.schedmd.com/slurmrestd.html). Listens for requests on port `6820`.

The `entrypoint.sh` scripts start the respective daemons.

`submitter-entrypoint.sh` is not used by any of these containers, but can be used to make a "submission node" (AKA "login node").
That means a node that has a Slurm installation and still runs [munged](https://linux.die.net/man/8/munged) so that the submission node can submit jobs, but it doesn't do work.
This is used by the parent docker-compose setup, to submit Slurm jobs from Prefect flows.

`slurm*.conf` are configs made using [the configurator](https://slurm.schedmd.com/configurator.html) for this docker compose setup only.

`slurm_prolog.sh` and `slurm_epilog.sh` are scripts that execute before and after a job, respectively.
We are using them to pretend that `/nfs/public` is only available to jobs running in the `datamover` partition.
This is to help with writing jobs for EBI Codon Slurm infrastructure, which looks a bit like this.

`jwt_hs256.key` is the key used for generating jwt tokens. Changing this will make your previous tokens invalid.

### Data and filesystems
There are dummy HPS and NFS filesystems at `/hps` and `/nfs/production`. On a `datamover` partitioned job, there is also `/nfs/public`.

## Using it
### Interactively on the nodes
The Slurm cluster is called `donco` (not `codon` :-] ).
You can dispatch Slurm commands on one of the nodes, e.g. the `slurm_node` (single node setup) or `slurm_worker` or `_controller` in the full setup.

```shell
user@host:/# task slurm
root@slurm_node:/# sinfo
# PARTITION AVAIL  TIMELIMIT  NODES  STATE NODELIST
# debug*       up   infinite      1   idle slurm_node
# datamover    up   infinite      1   idle slurm_node

root@slurm_node:/# sbatch --wait -t 00:00:30 --mem 10M --wrap="ls" -o listing.txt
# Submitted batch job 54
root@slurm_node:/# sacct -j 54
# JobID           JobName  Partition    Account  AllocCPUS      State ExitCode
# ------------ ---------- ---------- ---------- ---------- ---------- --------
# 54                 wrap      debug       root          1  COMPLETED      0:0
# 54.batch          batch                  root          1  COMPLETED      0:0
root@slurm_node:/# srun -t 1:00:00 --mem=100M --partition=datamover --pty bash -l
root@slurm_node:/# cd /nfs/public
root@slurm_node:/nfs/public#
# Note that this extra /nfs/public mount is available because we are on the datamover parition
```

### Via Taskfile convenience wrapper
From the parent dir, where there is a Taskfile:
`task slurm` will open a shell on the slurm controller node, where you can run things like `sinfo`, `squeue`, `srun`, `sbatch` etc. (as bove)

`task sbatch -- --wait -t 00:00:30 --mem 10M --wrap="ls" -o listing.txt` will dispatch a job directly.

Use e.g. `docker logs slurm_node -f` to see the job execute.


### Via the REST API
Inside the worker node generate a token
```shell
root@slurm_node:/# scontrol token username=slurm
SLURM_JWT=<generated-jwt-token>
```

Submit a request using curl or Postman
```shell
curl --location 'localhost:6820/slurm/v0.0.40/ping' \
--header 'X-SLURM-USER-NAME: slurm' \
--header 'X-SLURM-USER-TOKEN: <generated-jwt-token>'
```

Note: SLURM REST API comes in different versions, `v0.0.40` is used in this example.
More info [here](https://slurm.schedmd.com/rest.html).
