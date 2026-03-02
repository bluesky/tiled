# Globus Streams

## What is Globus Streams?

[Globus Streams][] is a premium feature available with a Globus subscription.
It can be used to open a tunnel over a wide-area network between two Globus
endpoints. From the user point of view, it is akin to an SSH tunnel, but
without SSH or the need for pre-deployed keys.

## How does Tiled integrate with it?

Some HPC compute nodes do not allow outbound connections, making public Tiled
servers inaccessible from those hosts. Globus Streams can bridge this gap by
tunneling requests to Tiled:

- A Globus Streams **listener**, running at an endpoint with connectivity
  to the Tiled server, routes traffic to it.
- A Globus Streams **initiator**, running on the compute node, routes
  requests from user software through the tunnel to the listener.

## Step by Step Guide

Create a tunnel at [https://app.globus.org/streams/create](https://app.globus.org/streams/create).
The _Initiator Access Point_ should be on the Tiled client side, and the
_Listener Access Point_ should be on the Tiled server side.
(The _Label_ is arbitrary.)

![Create Globus Stream](_static/create-globus-stream.png)

The new tunnel is displayed with a unique "tunnel ID" that we will need later.

![Globus Tunnel](_static/globus-tunnel.png)

Identify an IP address for the Tiled server of interest. As an example, we can
use a public IP of the public demo Tiled server, `192.203.218.28`.

On the listener side, on a host that _has_ connectivity to the Tiled server,
initialize a listener.

```sh
# Replace these example values with your own.
export TUNNEL_ID=6e1fba7b-f098-44ae-9b4e-e5a22facf0e5
export TILED_IP=192.203.218.28

globus-streams environment initialize --listener-contact-string ${TILED_IP}:443 ${TUNNEL_ID}
```

On the initiator side, on a host that needs access to Tiled, initialize an
environment for the tunnel.

```sh
# Replace this example value with your own.
export TUNNEL_ID=6e1fba7b-f098-44ae-9b4e-e5a22facf0e5
globus-streams environment initialize ${TUNNEL_ID}
```

Then run any commands or scripts that need access to Tiled prefixed by the
script `globus-streams-launch.sh`. (This script is provided by your
installation of Globus.)

As an example, just to check connectivity, we can use `curl` to fetch the Tiled
landing page.

```sh
/usr/share/globus/streams/globus-streams-launch.sh ${TUNNEL_ID} curl https://tiled-demo.nsls2.bnl.gov
```

[Globus Streams]: https://www.globus.org/streaming-data
