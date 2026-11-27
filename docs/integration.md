# Integrating sync-core

## Application boundary

The library owns encrypted signed changes, field-version merging, visible conflicts, SQLite projections and outboxes, incremental transport, retry and ancestor recovery. Each client has its own directory, namespace, identity, workspace and mutexes. There is no global active account.

Applications supply `Config.Namespace`, allowed device platforms, and deterministic operation/record validation callbacks. They retain schemas, business rules, UI, local application databases, access-code/session management, invitations and deployment. Legacy `NewWorkspace` accepts one owner and configurable initial editors. Native spaces use upstream coordinator sharing and network-accepted ACL membership.

The application server authorizes a workspace before calling `Exchange`, serializing exchanges for that workspace. Bootstrap bundles contain private keys; only the authenticated owner of an account may receive its bundle. The homeserver is trusted and holds account keys.

## Client contract

1. Create a client with `NewClient(directory, config)`. Use a separate private directory for each replica.
2. Call `Connect(context, origin, accessCode)` for sign-in or reauthentication. HTTPS is required except on loopback. Redirects are rejected. Existing replicas are pinned to their origin, namespace, account and workspace.
3. Build field `Operation` values with stable IDs. `Replaces` identifies the exact versions superseded. Commit outgoing application transactions durably before submitting them.
4. Call `Apply`. Success means signed changes, projections and the engine outbox committed together. Replay lost acknowledgements with the same IDs and payloads.
5. Run `Sync` on a worker. Local edits can commit during HTTP. Incoming pages, cursors and acknowledgements commit together. Project `View` records idempotently into the app store, preserving app edits not yet accepted by the engine.
6. Show `Record.Conflict(field)` and preserve every `Fields[field]` version. Resolution creates a new operation replacing the chosen version IDs. Independent fields merge automatically. Deletion and restoration are application-defined fields.
7. Close after work stops. Failed authentication never clears a replica or pending changes. Localize `ErrorCode` in the application; diagnostic text is not a UI resource.

Batches allow 200 fields and 200,000 encoded bytes. History is paged and retained without pruning. Compaction and large deployments require further work.

## Local migration staging

The upstream dependency pair is `any-sync v0.13.8` and `any-store v1.0.2`.
Do not substitute any-store v2: it is a different storage implementation and is
not the dependency used by this any-sync release.

`MigrationSpace(config, ownerBundle, replicationKey)` creates a deterministic,
owner-signed v1 envelope. Share that same public envelope and replication key
with the workspace's replicas; do not distribute the owner's private bundle.
`Client.StageMigration(ctx, destination, envelope)` holds the client locks and
copies the complete signed history plus **all** pending change IDs into
`destination/replica/space.sqlite` and `destination/replica/manifest.json`.
The low-level `StageMigration` function requires a quiescent source engine.

Staging requires a new destination. It validates the envelope and application
projection, compares signed history and heads, checkpoints storage, and verifies
the reopened copy before publication. It never switches the active client,
acknowledges outgoing writes, or deletes the source. Manifests explicitly carry
`status: "local-only"`; pending IDs are bookkeeping, not delivery receipts.
Protect staged directories like other private application data. Account private
keys and authentication tokens are not added to the manifest.

**Local staging is not admission to a self-hosted any-sync network.**
`ValidateMigrationACL(config, bundle, networkID)` checks the existing ACL against
the upstream verifier anchored to your private network's root key. It rejects an empty or malformed network ID
and returns `ErrMigrationACLUnaccepted` when the membership chain is not accepted.
Passing this preflight is necessary but does not prove coordinator admission,
network tree compatibility, or live/reconnect synchronization.

Legacy multi-member workspaces lack network-acceptor signatures. Adding those
signatures changes membership-record CIDs, while existing signed trees retain
references to the old ACL heads. Bypassing network verification or relabelling
records with their old IDs is not a valid migration. Keep the original
replica intact. `ReplayMigration` supports an archive-and-replay strategy without
weakening those checks; the existing HTTPS client is not automatically switched.

### Archive and replay into a new private space

1. Quiesce and reconcile the legacy workspace, including each installation's
   unsent changes. Archive its signed history and **all** pending IDs using
   `StageMigration`. Retain the active replica as a rollback copy.
2. Create a new private space with the original owner identity. Register it with
   the private coordinator, call `Coordinator.SpaceMakeShareable` for a shared
   workspace, then use `space.AclClient().AddAccounts` for the original writers.
   The private consensus service supplies acceptance signatures.
3. Create a new application tree root and obtain the accepted ACL records.
   `ReplayMigration(ctx, source, newRoot, aclRecords, networkID, signers)` checks
   the network root independently and re-signs the original atomic transactions
   in causal order using **each original author's own account key**. It runs in
   isolated memory and returns no partial payload on failure.
4. Import the returned `Payload` with `Host.PutTree`, attach the application
   replica, and independently verify the remote projection before cutover.
   `Changes` maps old signed-change IDs to new ones; this map is **not** proof of
   remote durability and must not itself clear an outbox.

Application operation IDs, authors, source-device metadata, replacement links,
conflicts and superseded operations are retained and the complete resulting
view is compared before returning. Space, tree and signed-change IDs are new;
old signed history remains in the archive. Missing original signing keys,
revoked writers, schema failures, or an unaccepted ACL block replay rather than
silently changing authorship. Use keys already held by the application's trusted
gateway; do not distribute users' keys to run a migration. This operation does
not collect edits from disconnected installations or implement cutover/rollback
coordination for the application.

## Private network replicas

`privatenetwork.New(configYAML, expectedNetworkID, directory, accountKeys)`
assembles upstream secure peer handshakes, TCP/yamux and QUIC transports,
connection pooling, coordinator access, and topology discovery. Supply the
network configuration and pinned network identity from your own administrator.
There are no built-in public endpoints or fallback network. Both cached and
newly discovered topology must retain the pinned network identity.

Attach a host before starting the runtime:

```go
runtime, err := privatenetwork.New(configYAML, expectedNetworkID, directory, keys)
// Handle err after each call.
host, err := privatenetwork.AttachSpaces(runtime, directory)
err = runtime.App.Start(ctx)
space, err := host.OpenSpace(ctx, spaceID)
tree, err := host.GetTree(ctx, space.Id(), treeID)
```

`Host.CreateSpace` creates a private owner-signed space. Use the default upstream
space type and put application metadata in `SpacePayload`; coordinator admission
does not accept arbitrary application namespaces as space types. Application
trees use their own namespace as `ChangeType`. Initialize a new empty application
tree with `InitializeReplicaTree(ctx, config, root, acl, keys)` before importing
it: upstream intentionally does not replicate root-only trees. The seed is a
signed, encrypted empty operation batch, not a fabricated user record.
`PutTree` validates and imports
signed history; `GetTree` opens persisted history or fetches it from the private
space's responsible nodes. Only explicitly opened spaces accept incoming traffic.

Local writes broadcast over persistent upstream streams. Idle replicas subscribe
to changes, including when a stream is recreated. Invoke `Host.Reconnect(ctx)` on
connectivity/resume events to reconcile gaps. Retain upstream's 600-second
anti-entropy safety interval; do not add an application synchronization timer.
Close the host before the runtime, after application workers stop using trees.

`AttachReplica(config, syncTree, keys, user, members)` adds the shared operation
schema and conflict projection to an upstream `synctree.SyncTree`. `Apply(ctx,
batch)` commits locally and queues broadcast. Replaying identical operations is
idempotent. Consume `Changes()` on a worker and read `View()` to update the app
store. Notifications are coalesced invalidations, not remote delivery receipts.
`Replica` owns the tree listener and rejects invalid incremental remote
operations before storage commit. Do not replace its listener or enable deferred
updates. Validate initial imports before activation: attaching a replica cannot
undo history already imported by its caller. Membership labels come from trusted
workspace configuration; upstream ACL/signature validation remains authoritative.

Neither attaching a host nor a replica switches a legacy HTTPS client, converts
its membership, or acknowledges its pending outbox. Keep legacy replicas intact
until application-specific migration and activation have been validated.

### Application-facing native client

`privateclient.Open(ctx, directory, config, networkYAML, networkID, owner,
accountKeys, signedConnection)` owns the runtime, space and validated projection.
Provision `SignedConnection` with `privateclient.SignConnection` using the
workspace owner's key and deliver it through authenticated application pairing.
Independently trust the network ID and owner; merely copying them from an
untrusted connection is not verification. The signature binds the namespace,
space, tree and account-to-label mapping. Opening also checks the real space
owner, account permissions, root signer and namespace.

The client pins its directory to the account and connection. It stores no keys
or session tokens; applications supply their own account/key storage. Use a
separate directory from the legacy client and keep the signed connection stable.
Account/workspace changes require explicit provisioning, not silent rebinding.

- `Apply(ctx, operations)` commits locally and broadcasts through upstream.
- `Changes()` signals coalesced invalidations; consume on a worker, then read
  `View()` and update the application's local store idempotently.
- `Reconnect(ctx)` is for connectivity/resume events, not a timer.
- `Done()` unblocks workers when `Close(ctx)` shuts down the client. Close is
  idempotent and serializes with active application calls.

Opening a persisted replica does not require reachable nodes. Upstream's
initial reconciliation runs in the background, while cached data and durable
local writes remain available offline. First-time pairing still needs the
private services. Neither a local commit nor a change notification is a remote
receipt. This API does not replace access-code provisioning or UI localization.

For a loopback-only development deployment, run:

```sh
python3 scripts/private-network.py prepare --directory /absolute/private/runtime-directory
python3 scripts/private-network.py start --directory /absolute/private/runtime-directory
python3 scripts/private-network.py status --directory /absolute/private/runtime-directory
python3 scripts/private-network.py stop --directory /absolute/private/runtime-directory
```

Use a new directory outside source checkouts. Generated service keys and
storage stay there; stopping does not delete them. This is a development
network, not a production deployment configuration.

The opt-in `TestPrivateNetworkHandshake` uses synthetic account keys. Set
`SYNC_CORE_PRIVATE_CONFIG` to your development `etc/client.yml` and
`SYNC_CORE_PRIVATE_NETWORK_ID` to its independently trusted network identity,
then run `go test -race ./... -run TestPrivate`.
`TestPrivateSpaceLiveReplication` covers bidirectional delivery, restart and
concurrent offline branches on persisted any-store databases.
`TestPrivateApplicationReplica` exercises application operations and change
notifications over those real services. `TestPrivateMembership` checks independent
accounts, rejected permission escalation, and outsider isolation.
`TestPrivateLegacyMigration` archives and replays a conflicted two-author
workspace with 205 pending signed writes, then resolves a conflict over live
streams. `TestPrivateClientLifecycle` checks pairing, notifications, restart,
entirely offline edits and subsequent propagation using the application-facing
client. The tests create synthetic workspaces;
never point them at production. Ordinary unit tests do not require Docker.

## Gateway integration and deployment

`appclient.New` retains a legacy replica until authenticated provisioning is
available. Pairing, and the first explicit sync after startup, attempt verified
activation once; there is no provisioning poll. Local history and pending writes
must be reconciled before the native activation marker is written. Native data
uses a separate directory, with the legacy replica retained for rollback.
`Pending == -1` means node durability is not measured, not an empty outbox.

A trusted gateway can use `ReconcileLegacy` to bridge older installations and
browser clients with native replicas while preserving operation IDs, authors,
source devices and conflict ancestry. Commit its returned projection before
acknowledging a legacy exchange or notifying SSE subscribers. The gateway holds
account keys under the same trust model as the original applications; this is
not end-to-end encryption against that gateway.

With the gateway stopped and its existing database backed up, the applications'
`prepare-native` server command accepts `SYNC_NETWORK_CONFIG`, `SYNC_NETWORK_ID`
and `SYNC_NATIVE_DIRECTORY`. It archives signed history, creates accepted
membership, replays operations and verifies an independent remote replica before
writing `provision.json`. Install that public signed configuration as
`native-provision.json` in the application's data directory, then restart the
gateway. Original data and archive remain intact. Network configuration alone
contains no private keys; never export the service account configurations.

`scripts/hosted-network.py` prepares a self-hosted service deployment with an
explicit administrator-controlled `--advertise-host`. Unlike the loopback
wrapper, it publishes authenticated TCP ports 19001–19006 and QUIC ports
19101–19106. Databases, storage and administrative ports remain internal. Keep
its runtime directory private and outside source checkouts; back up its keys and
storage as part of the server's private backup policy. It never joins or falls
back to an Anytype-operated network.

## Validation and versioning

Run `go test -race ./...` and `go vet ./...`. Tests cover compatible signed history with synthetic accounts, conflicts, one-owner and multiple-editor workspaces, namespaces, offline restart, replay, edits during HTTP, and failed durable commits. Fixture keys are synthetic test material.

Consumers pin a release tag or commit.
