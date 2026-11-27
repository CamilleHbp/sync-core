# sync-core

A Go library for apps that need to keep working offline. Each device has a local replica. The app can save an edit immediately and exchange signed changes with the other replicas when a connection returns.

[Integrate the Go client](docs/integration.md#client-contract)

## What happens when two people edit a record?

Suppose two people open the same task offline. One changes its title and the other changes its due date. On reconnect, both edits survive because they touch different fields. If both people change the title, the record carries both versions. Your app can show them side by side and save the user’s choice.

## Add it to an app

Pin `github.com/CamilleHbp/sync-core` to a release or commit. Give each replica a private directory and a `Config` with your application namespace and validation rules.

1. Open the client and pair it with the workspace over HTTPS. The initial pairing needs a connection.
2. Pass field edits to `Apply`. Give each operation a stable ID so a retry can safely submit the same edit. A local commit survives an app restart.
3. Run `Sync` on a worker and read `View` to update your interface. If `Record.Conflict(field)` is true, keep the competing versions until the user resolves them.

The library handles signed history, SQLite storage, replay and field conflicts. Your app defines the record schema, sign-in experience and conflict controls. The HTTPS gateway holds account keys and must be trusted. The private-network client can connect to a self-hosted any-sync network.

[Client contract and limits](docs/integration.md#client-contract) · [Private network setup](docs/integration.md#private-network-replicas) · [Migrating an existing replica](docs/integration.md#local-migration-staging)

## Development

Run `go test -race ./...` and `go vet ./...` from the module root. The ordinary tests run without Docker. The [integration guide](docs/integration.md) includes the opt-in tests for real private-network peers.

## License

sync-core uses the [Apache License 2.0](LICENSE). The any-sync components retain their [MIT license](LICENSE.any-sync). See [NOTICE](NOTICE) for attribution.
