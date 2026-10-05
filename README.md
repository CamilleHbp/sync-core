# sync-core

Reusable Go synchronization engine extracted from Groceries, built on pinned upstream any-sync object trees. Android applications can wrap it with gomobile; desktop applications can host it in a helper process. It uses trusted application homeservers over HTTPS, not the official any-sync coordinator/DRPC network.

## Application boundary

The library owns encrypted signed changes, field-version merging, visible conflicts, SQLite projections and outboxes, incremental transport, retry and ancestor recovery. Each client has its own directory, namespace, identity, workspace and mutexes. There is no global active account.

Applications supply `Config.Namespace`, allowed device platforms, and deterministic operation/record validation callbacks. They retain schemas, business rules, UI, local application databases, access-code/session management, invitations and deployment. `NewWorkspace` accepts one owner and configurable initial editors. Runtime membership management is not part of this initial API.

The application server authorizes a workspace before calling `Exchange`, serializing exchanges for that workspace. Bootstrap bundles contain private keys; only the authenticated owner of an account may receive its bundle. The homeserver is trusted and holds account keys, matching the original Groceries model.

## Client contract

1. Create a client with `NewClient(directory, config)`. Use a separate private directory for each replica.
2. Call `Connect(context, origin, accessCode)` for sign-in or reauthentication. HTTPS is required except on loopback. Redirects are rejected. Existing replicas are pinned to their origin, namespace, account and workspace.
3. Build field `Operation` values with stable IDs. `Replaces` identifies the exact versions superseded. Commit outgoing application transactions durably before submitting them.
4. Call `Apply`. Success means signed changes, projections and the engine outbox committed together. Replay lost acknowledgements with the same IDs and payloads.
5. Run `Sync` on a worker. Local edits can commit during HTTP. Incoming pages, cursors and acknowledgements commit together. Project `View` records idempotently into the app store, preserving app edits not yet accepted by the engine.
6. Show `Record.Conflict(field)` and preserve every `Fields[field]` version. Resolution creates a new operation replacing the chosen version IDs. Independent fields merge automatically. Deletion and restoration are application-defined fields.
7. Close after work stops. Failed authentication never clears a replica or pending changes. Localize `ErrorCode` in the application; diagnostic text is not a UI resource.

Batches allow 200 fields and 200,000 encoded bytes. History is paged and retained without pruning. Compaction and large deployments require further work.

## Validation and versioning

Run `go test -race ./...` and `go vet ./...`. Tests cover Groceries-generated signed history with synthetic accounts, conflicts, one-owner and multiple-editor workspaces, namespaces, offline restart, replay, edits during HTTP, and failed durable commits. Fixture keys are synthetic test material.

Consumers pin a release tag or commit. Groceries retains its embedded engine until its separate migration is validated.
