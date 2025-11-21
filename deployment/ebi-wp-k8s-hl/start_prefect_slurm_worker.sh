#!/bin/bash

DJANGOPIDFILE=/nfs/production/rdf/metagenomics/jenkins-slurm/processes/dev-prefect-slurm-worker.pid

source /hps/software/users/rdf/metagenomics/service-team/repos/mi-automation/team_environments/codon/mitrc.sh

source /nfs/production/rdf/metagenomics/jenkins-slurm/dev-prefect-agent/deployment/ebi-wp-k8s-hl/secrets-dev-mi-slurm-worker.env

source /nfs/production/rdf/metagenomics/jenkins-slurm/dev-prefect-agent/venv/bin/activate

cd /nfs/production/rdf/metagenomics/jenkins-slurm/dev-prefect-agent/
python manage.py prefectcli worker start --type "slurm" --pool "slurm_2" >> /nfs/production/rdf/metagenomics/jenkins-slurm/logs/dev-prefect-slurm-worker.log 2>&1 &

echo $! > "${DJANGOPIDFILE}"
