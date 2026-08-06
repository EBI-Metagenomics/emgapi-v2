# Deployment configuration

Production and shared-environment deployment configuration is intentionally not
kept in this application repository.

Kubernetes manifests, migration jobs, production Prefect deployment declarations,
cluster-specific Taskfiles, worker supervisor scripts, ingress rules, NFS paths,
namespaces, kubeconfig references, pull-secret references, and secret runbooks
belong in the private infrastructure repository.

This repository should contain only application code, local development tooling,
tests, and container build inputs.
