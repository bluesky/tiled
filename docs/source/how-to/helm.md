# Run Tiled Server from the Helm chart

There is an official Tiled helm chart image for use with
[Helm](https://helm.sh/) and [Kubernetes](https://kubernetes.io/).

## Quickstart

This quickstart guide installs the Tiled server configured with a single API key
with permissions to write and access all data. It is not intended for production use,
and is just a useful default with minimal configuration.

Installing the Helm chart with default values:

```
helm install tiled oci://ghcr.io/bluesky/charts/tiled
```

This Helm chart additionally refers to, but does not install, a Secret that contains
the value to use as the API key. It is highly recommended to use a 
[SealedSecret](https://github.com/bitnami-labs/sealed-secrets#readme) if kubeseal is
available on the cluster you are installing into.

See the [Docker documentation](./docker.md) for instructions on creating a secure key.

```sh
# This will only decrypt in your namespace, in your cluster, and is safe to commit
$ echo -n TILED_SINGLE_USER_API_KEY=<key> | kubeseal --raw --namespace <your namespace> --name tiled-secrets
AgCC...
```

Use the result as the body of a SealedSecret:

```yaml
apiVersion: bitnami.com/v1alpha1
kind: SealedSecret
metadata:
  name: tiled-secrets
spec:
  encryptedData:
    tiled-secrets: AgCC...
```

And either apply the SealedSecret to the cluster with kubernetes:

```sh
kubectl apply -f secret.yaml
```

Or add it to a chart that manages the deployment:


```
(
  | Chart.yaml
  | values.yaml
  | \templates
    | secret.yaml
```