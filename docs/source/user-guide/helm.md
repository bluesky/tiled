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

This Helm chart additionally refers to, but does not create, a Secret that contains
the value to use as the API key. It is highly recommended to use a
[SealedSecret](https://github.com/bitnami-labs/sealed-secrets#readme) if kubeseal is
available on the cluster you are installing into.

See the [Docker documentation](./docker.md) for instructions on creating a secure key.

```sh
# This will only decrypt in your namespace, in your cluster, and is safe to commit
$ echo -n <key> | kubeseal --raw --namespace <your namespace> --name tiled-secrets
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
    TILED_SINGLE_USER_API_KEY: AgCC...
```

Apply the SealedSecret to the cluster with kubernetes:

```sh
kubectl apply -f secret.yaml
```

## Further Configuration

A common pattern for managing the configuration of a Helm chart is to wrap the config
as another layer of chart, with the bundled instance configuration and the dependent
charts kept under source control.

```
(
  | Chart.yaml
  | values.yaml
  | \templates
    | secret.yaml
  | .git
```

The dependent chart(s): tiled and any other services that *should live and die with the
tiled instance* can be referenced from the Chart.yaml:

```yaml
apiVersion: v2
name: my-install-of-tiled
description: tiled configured for use at...

version: 0.1.0
appVersion: v0.1.0b12

type: application

dependencies:
# Fetches the tiled Helm chart with version 0.1.0
  - name: tiled
    version: "0.1.0"
    repository: "oci://ghcr.io/bluesky/charts"
```

While overrides for the bundled values.yaml in each dependency chart can be passed
as part of the values.yaml.

Note that the `name` in the `dependencies` in the Chart.yaml give the top-level key
to use in the values.yaml:

```yaml
tiled:
  # This is mounted as config.yaml to the tiled container
  # Replacing `config` value in the helm/tiled/values.yaml file in this repository
  config: {}
```

Additional templates to be deployed alongside the tiled server can be defined- for
example the SealedSecret defined above.
