#!/bin/bash
set -e

echo "ℹ️ Ensure ownership of munge key"
chown -R munge:munge /etc/munge

echo "ℹ️ Start munged for auth"
gosu munge /usr/sbin/munged

echo "ℹ️ Start slurm rest api daemon"

slurmrestd -v 0.0.0.0:6820

exec "$@"
