#!/bin/bash

# This file is sourced by prefect-slurm, so shell options remain active while
# the generated `prefect flow-run execute` command runs.
set -e
source /opt/venv/bin/activate
command -v prefect >/dev/null
