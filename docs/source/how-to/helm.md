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

## Remote debugging

The helm chart defines values for enabling debugpy breakpoints inside of the deployed
container, for use in the situation that cluster infrastructure is required for
debugging: e.g. checking the value of headers passed from authentication providers.

```yaml
debug:
  # Whether the container should start in debug mode
  enabled: true
  # Whether to suspend the process until a debugger connects
  suspend: true
  # Port to listen for the debugger on
  port: 5678
```

To connect to the port of the pod on the cluster, you can use `kubectl port forward` to
forward your local machine's port 5678 to the container's:

```sh
$ kubectl get pods
NAME                            READY   STATUS    RESTARTS        AGE
tiled-cbbc9df58-hjm7q           1/1     Running   0 (5d19h ago)   5d19h
$ kubectl port forward pod/tiled-cbbc9df58-hjm7q 5678:5678
Forwarding from 127.0.0.1:5678 -> 5678
```

Check out the deployed version of tiled and configure your IDE to attach to a remote
debugpy process: the following is a launch configuration from VSCode `launch.json`.
Note that the version of python and port may need to be changed. With this
configuration, `"justMyCode": False` was required for breakpoints to be active.

```json
{
    "name": "Python Debugger: Remote Attach",
    "type": "debugpy",
    "request": "attach",
    "connect": {
        "host": "localhost",
        "port": 5678
    },
    "pathMappings": [
        {
            "localRoot": "${workspaceFolder}/tiled",
            "remoteRoot": "/opt/venv/lib/python3.12/site-packages/tiled"
        }
    ],
    "justMyCode": false
}
```
