#!/bin/bash
set -e

echo "ℹ️ Ensure ownership of munge key"
chown -R munge:munge /etc/munge

echo "ℹ️ Start munged for auth"
gosu munge /usr/sbin/munged

echo "ℹ️ Start mitmdump for intercepting"
mitmdump --mode regular -s /app/slurm-dev-environment/debug_tools/prefect_agent_mitm.py &

echo "ℹ️ Helpers all started"
exec "$@"
