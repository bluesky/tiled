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

## Deploying with oauth2-proxy

Deploying behind a reverse proxy that redirects unauthenticated requests to your OAuth2/OIDC provider places a layer of security in front of the tiled API, and allows authenticated requests in the web frontend with a full OIDC flow on your provider's login page.

Configure your OAuth2 client values, referencing back to the Quickstart guide for how to configure a SealedSecret in your templates directory.

```{note}
The following assumes that your tiled installation is configured with an umbrella chart as described in the Further Configuration section.
It adds the oauth2-proxy helm chart as a dependency of the umbrella chart- the proxy will live and die with the tiled instance.
It makes use of [the currently unstable alphaConfig](https://oauth2-proxy.github.io/oauth2-proxy/configuration/alpha-config/
), to allow it to pass Authorization headers into the tiled pod.
```


```yaml
dependencies:
  - name: tiled
    version: "0.1.0"
    repository: "oci://ghcr.io/bluesky/charts"
  - name: oauth2-proxy
    version: "~7.10.2" # >=7.10.2 < 7.11
    repository: "https://oauth2-proxy.github.io/manifests"
```

Add required configuration as SealedSecrets as appropriate.

```yaml
apiVersion: bitnami.com/v1alpha1
kind: SealedSecret
metadata:
  name: tiled-secrets
spec:
  encryptedData:
    CLIENT_SECRET: AgCC...
```

Ensure that tiled is not accessible directly, and configure the reverse proxy.

```yaml
tiled:
  ...
  ingress:  # Move ingress/LoadBalancer configuration into the oauth2-proxy configuration
    enabled: false
  service:
    type: ClusterIP

oauth2-proxy:
  extraEnv:
    - name: CLIENT_SECRET
      valueFrom:
        secretKeyRef:
          name: tiled-secrets
          key: client-secret
  ingress: {}  # Configure your Ingress/LoadBalancer to point to the oauth2-proxy pod
  config:
    configFile: |-  # Cannot be empty else tries to define invalid upstreams
      email_domains = [ "*" ]
      skip_provider_button = true
  alphaConfig:
    enabled: true
    configData: # https://oauth2-proxy.github.io/oauth2-proxy/configuration/alpha-config/
      upstreamConfig:
        proxyRawPath: true
        upstreams:
          - id: tiled
            path: /
            uri: http://tiled # Assuming tiled dependency AND helm deployment are named "tiled"
      providers: [] # Configure your OAuth2 provider here
      injectRequestHeaders:
        - name: Authorization  # Passes header into pod
          values:
            - claim: access_token
              prefix: "Bearer "

```

Configure tiled to use an OAuth2 compatible authentication method, such as the OIDCAuthenticator

```yaml
tiled:
  ...
  # mount CLIENT_SECRET and CLIENT_ID as env vars
  extraEnvVars:
    - name: CLIENT_SECRET
      valueFrom:
        secretKeyRef:
          name: tiled-secrets
          key: client-secret

  config:
    authentication:
      providers:
      # Configure the OIDCAuthenticator
      - provider: example.com
        authenticator: tiled.authenticators:OIDCAuthenticator
        args:
          audience: tiled  # something unique to ensure received headers are for you
          client_id: tiled
          client_secret: ${CLIENT_SECRET}
          well_known_uri: https://example.com/.well-known/openid-configuration
          confirmation_message: "You have logged in with example.com as {id}."
