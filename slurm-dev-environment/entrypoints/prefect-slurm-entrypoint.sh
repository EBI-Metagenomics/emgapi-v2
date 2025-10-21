#!/bin/bash
set -e

# echo "ℹ️ Ensure ownership of munge key"
# chown -R munge:munge /etc/munge

echo "ℹ️ Start munged for auth"
gosu munge /usr/sbin/munged

echo "ℹ️ Create initial token file"
gosu slurm /bin/bash -c "scontrol token username=slurm lifespan=4000 | prefect-slurm token"

echo "ℹ️ Start token update cronjob"
# Runs every 60mins, token lifespan is 66mins
echo "0 * * * * /bin/bash -c 'source /opt/venv/bin/activate && scontrol token username=slurm lifespan=4000 | prefect-slurm token' >> /tmp/token_refresh.log 2>&1" | crontab -u slurm -
service cron start

gosu slurm "$@"
