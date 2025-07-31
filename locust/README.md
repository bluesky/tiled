# Tiled Load Testing

Load testing for the Tiled data management system.

## Quick Start

```bash
pixi install
pixi run locust -f locustfile.py --host http://localhost:8000
```

## User Types

- **TiledUser**: Tests basic endpoints like root and metadata with API key authentication

## Configuration

```bash
# Set API key (default: secret)
TILED_SINGLE_USER_API_KEY=secret pixi run locust -f locustfile.py --host http://localhost:8000

# Headless mode (100 users, 10/sec spawn rate, 60s duration)
pixi run locust -f locustfile.py --host http://localhost:8000 --headless -u 100 -r 10 -t 60s

# Reduce logging noise
pixi run locust -f locustfile.py --host http://localhost:8000 -L WARNING
```

## Kubernetes Testing

```bash
# Deploy Tiled and PostgreSQL
kubectl apply -f kube/postgres-deployment.yml
kubectl apply -f kube/tiled.yml

# Initialize database
kubectl exec -it deployment/postgres -- psql -U postgres -c "CREATE USER tiled WITH SUPERUSER PASSWORD 'secret';"
kubectl exec -it deployment/postgres -- psql -U postgres -c "CREATE DATABASE catalog ENCODING 'utf-8' OWNER tiled;"
kubectl exec -it deployment/postgres -- psql -U postgres -c "CREATE DATABASE storage ENCODING 'utf-8' OWNER tiled;"

# Port forward and test
kubectl port-forward svc/tiled 8000:8000
pixi run locust -f locustfile.py --host http://localhost:8000
```

## PostgreSQL Commands

```bash
# Connect to PostgreSQL
kubectl port-forward svc/postgres-service 5432:5432
psql -h localhost -U postgres -d tiled

# Check database contents
kubectl exec -it deployment/postgres -- psql -U postgres -c "\l"
```

## Monitoring Resources

```bash
# Monitor Tiled and PostgreSQL resources
watch -n 5 ./monitor_resources.sh
```

## Remove Deployments

```bash
kubectl delete -f kube/tiled.yml
kubectl delete -f kube/postgres-deployment.yml
```