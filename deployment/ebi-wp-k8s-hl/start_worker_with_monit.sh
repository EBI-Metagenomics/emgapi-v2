#!/bin/bash

DJANGOPIDFILE=/nfs/production/rdf/metagenomics/jenkins-slurm/processes/dev-mi-slurm-worker-prefect-worker.pid

source /hps/software/users/rdf/metagenomics/service-team/repos/mi-automation/team_environments/codon/mitrc.sh

source /nfs/production/rdf/metagenomics/jenkins-slurm/dev-prefect-agent/deployment/ebi-wp-k8s-hl/secrets-dev-mi-slurm-worker.env

source /nfs/production/rdf/metagenomics/jenkins-slurm/dev-prefect-agent/venv/bin/activate

cd /nfs/production/rdf/metagenomics/jenkins-slurm/dev-prefect-agent/
env HTTP_PROXY="http://127.0.0.1:8080" python manage.py prefectcli worker start --pool "slurm" >> /nfs/production/rdf/metagenomics/jenkins-slurm/logs/dev-mi-slurm-worker-prefect-worker.log 2>&1 &

echo $! > "${DJANGOPIDFILE}"
