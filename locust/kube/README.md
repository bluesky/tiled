# PostgreSQL Kubernetes Deployment

## Deploy PostgreSQL

Apply the PostgreSQL deployment:

```bash
kubectl apply -f postgres-deployment.yml
```

## Verify Deployment

Check if the pods are running:

```bash
kubectl get pods -l app=postgres
```

Check the service:

```bash
kubectl get svc postgres-service
```

## Connect to PostgreSQL

Forward the port to access PostgreSQL locally:

```bash
kubectl port-forward svc/postgres-service 5432:5432
```

Connect using psql:

```bash
psql -h localhost -U postgres -d tiled
```

Password: `secret`

## Access Tiled

Forward the port to access Tiled locally:

```bash
kubectl port-forward svc/tiled 8000:8000
```

Then open http://localhost:8000 in your browser.

## Initialize Database

Run the initialization script:

```bash
kubectl exec -it deployment/postgres -- psql -U postgres -c "CREATE USER tiled WITH SUPERUSER PASSWORD 'secret';"
kubectl exec -it deployment/postgres -- psql -U postgres -c "CREATE DATABASE catalog ENCODING 'utf-8' OWNER tiled;"
kubectl exec -it deployment/postgres -- psql -U postgres -c "CREATE DATABASE storage ENCODING 'utf-8' OWNER tiled;"
```

## Remove Deployment

To remove the PostgreSQL deployment:

```bash
kubectl delete -f postgres-deployment.yml
```