# Self-host synchronization

For Tasks, run the workspace service and the web gateway on your own host. The gateway serves the web app and forwards Android sync requests. Everyone using a workspace enters the same HTTPS address on their devices. The workspace code controls access.

## Run a Tasks workspace

You’ll need Git, the Go version specified in [the Tasks module](https://github.com/CamilleHbp/Tasks/blob/main/sync-engine/go.mod), Node.js 22.12 or later, and a domain you control. Run these commands as the account that will own the service data.

```sh
git clone https://github.com/CamilleHbp/Tasks.git
cd Tasks
(cd pwa && npm ci && npm run build)
(cd sync-engine && GOWORK=off go build -o ../tasks-workspace ./cmd/server)
(cd pwa/server && GOWORK=off go build -o ../../tasks-web .)
install -d -m 700 "$HOME/tasks-data/workspace" "$HOME/tasks-data/web"
```

From the checkout root, start the workspace service. On its first run it creates a workspace and writes its code to `access-code.json` inside the workspace data directory.

```sh
DATA_DIR="$HOME/tasks-data/workspace" LISTEN_ADDR=127.0.0.1:8790 ./tasks-workspace
```

In another terminal, start the gateway from the same checkout root. Replace `tasks.example.org` with your domain.

```sh
PUBLIC_ORIGIN=https://tasks.example.org SYNC_ORIGIN=http://127.0.0.1:8790 \
  DATA_DIR="$HOME/tasks-data/web" ASSET_DIR="$PWD/pwa/dist" \
  LISTEN_ADDR=127.0.0.1:8791 ./tasks-web
```

Point your domain’s DNS at your server. Configure your HTTPS reverse proxy to forward requests to port 8791. For example, a Caddy site on the same host can use this configuration.

```caddyfile
tasks.example.org {
    reverse_proxy 127.0.0.1:8791
}
```

Open your HTTPS site and check that `/healthz` returns HTTP 200. In **Synchronization** on the web and Android, enter that same site address and the code from the private workspace file (without its JSON quotes). Add a sample task on one device and check that it appears on the other.

For ongoing use, run both processes under a service manager with the same environment and absolute paths. Back up both private data directories with the services stopped. They contain account keys, session information and workspace data.

## Run a private any-sync network

Apps using sync-core’s native replication can use their own any-sync nodes. The commands below provision the network services. Each app supplies its own gateway and workspace provisioning.

On a server with Git, Python 3.9 or later, Docker and Docker Compose, run these commands from the sync-core checkout. Choose a new directory outside all Git checkouts. Replace the example hostname with one reachable by your clients.

```sh
network_dir="$HOME/any-sync-network"
python3 scripts/hosted-network.py prepare --directory "$network_dir" --advertise-host sync.example.org
python3 scripts/hosted-network.py start --directory "$network_dir"
python3 scripts/hosted-network.py status --directory "$network_dir"
```

Allow TCP ports 19001–19006 and UDP ports 19101–19106 between clients and the advertised host. After startup, the client network configuration is in `etc/client.yml` inside the network directory. Keep the service keys and storage private. Follow the [private network integration guide](integration.md#private-network-replicas) to attach a replica and pin the network identity. To stop the services, run `python3 scripts/hosted-network.py stop --directory "$network_dir"`.
