#!/bin/bash
source /nfs/production/rdf/metagenomics/jenkins-slurm/dev-prefect-agent/venv/bin/activate

slurm-tools api-token revoke --tag prefect
slurm-tools api-token generate --duration 25h --tag prefect | prefect-slurm token
