# Deploying to WebProd Kubernetes at EBI

## Regular redeployments:
Build the specific dockerfile for WP Kubernetes, push it to quay, and restart the app on WP K8s:
`task ebi-wp-k8s-hl:update-api`

To deploy a newly written flow, for example:
`FLOW=assemble_study task ebi-wp-k8s-hl:deploy-flow`
This *ONLY* deploys the flow to the prefect server. It doesn't mean the workers yet have access to that code.

If any code related to prefect flows has changed, or if the DB schema has changed, you'll also need to restart any prefect workers.
Look for a `deploy-prefect-worker` job on the Microbiome Informatics Jenkins, to do so.

## Initial deployment:

### Create a `secrets-k8s.env` file
```bash
DJANGO_SECRET_KEY=...
DATABASE_URL=postgres://...:...@app-database:5432/...
POSTGRES_APP_USER=...
POSTGRES_APP_PASSWORD=...
POSTGRES_PREFECT_USER=...
POSTGRES_PREFECT_PASSWORD=...
PREFECT_API_DATABASE_CONNECTION_URL=postgresql+asyncpg://...:...@postgres-prefect:5432/...
PREFECT_SERVER_API_AUTH_STRING=...<username>:<password>...
```

Make the secrets
`kubectl create secret generic emgapi-secret --from-env-file=secrets-k8s.env -n emgapiv2-hl-exp`

### Create Quay.io pull secrets
* Get authentication credentials for quay.io (the built image is private). You can get a Kubernetes secrets yaml file from your Quay.io user settings, in the "CLI Password" section.
* Download the secrets yaml and name the secret `name: quay-pull-secret` in the metadata section. Give it the right namespace. Put it into this folder.
* `kubectl apply -f secrets-quayio.yml`

### Deploy
`kubectl apply -f ebi-wp-k8s-hl.yaml`

### Migrate
`task migrate`
