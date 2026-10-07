package appclient

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	core "github.com/CamilleHbp/sync-core"
	"github.com/CamilleHbp/sync-core/privateclient"
	"github.com/CamilleHbp/sync-core/privatenetwork"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/spacepayloads"
	"github.com/anyproto/any-sync/util/crypto"
)

// Prepare archives and verifies a gateway workspace before publishing public
// provisioning metadata. Run with the gateway stopped and its SQLite backup
// retained. Source/bundles remain untouched. destination must not exist.
func Prepare(ctx context.Context, config core.Config, source *core.Engine, bundles []core.Bundle, network []byte, networkID, destination string) (Provision, error) {
	var provision Provision
	if _, err := privatenetwork.ParseConfig(network, networkID); err != nil {
		return provision, err
	}
	if source == nil || len(bundles) == 0 || destination == "" {
		return provision, core.NewError("invalid_config", nil)
	}
	if err := os.Mkdir(destination, 0700); err != nil {
		return provision, err
	}
	envelope, err := core.MigrationSpace(config, bundles[0], 1)
	if err != nil {
		return provision, err
	}
	if _, err = core.StageMigration(ctx, filepath.Join(destination, "archive"), source, envelope, nil); err != nil {
		return provision, err
	}
	signers := map[string]crypto.PrivKey{}
	for _, bundle := range bundles {
		key, err := crypto.UnmarshalEd25519PrivateKeyProto(bundle.Key)
		if err != nil {
			return provision, err
		}
		signers[key.GetPublic().Account()] = key
	}
	owner, err := crypto.UnmarshalEd25519PrivateKeyProto(bundles[0].Key)
	if err != nil {
		return provision, err
	}
	peer, err := accountdata.NewRandom()
	if err != nil {
		return provision, err
	}
	keys := accountdata.New(peer.PeerKey, owner)
	runtime, err := privatenetwork.New(network, networkID, filepath.Join(destination, "publisher"), keys)
	if err != nil {
		return provision, err
	}
	host, err := privatenetwork.AttachSpaces(runtime, filepath.Join(destination, "publisher"))
	if err != nil {
		return provision, err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		_ = host.Close(closeCtx)
		_ = runtime.Close(closeCtx)
	}()
	if err = runtime.App.Start(ctx); err != nil {
		return provision, err
	}
	master, _, err := crypto.GenerateRandomEd25519KeyPair()
	if err != nil {
		return provision, err
	}
	metadata, _, err := crypto.GenerateRandomEd25519KeyPair()
	if err != nil {
		return provision, err
	}
	space, err := host.CreateSpace(ctx, spacepayloads.SpaceCreatePayload{SigningKey: owner, MasterKey: master, MetadataKey: metadata, ReadKey: crypto.NewAES(), SpacePayload: []byte(config.Namespace)})
	if err != nil {
		return provision, err
	}
	if err = space.SyncHeads(ctx); err != nil {
		return provision, err
	}
	if len(bundles) > 1 {
		if err = runtime.Coordinator.SpaceMakeShareable(ctx, space.Id()); err != nil {
			return provision, err
		}
		additions := []list.AccountAdd{}
		for account, key := range signers {
			if account != owner.GetPublic().Account() {
				additions = append(additions, list.AccountAdd{Identity: key.GetPublic(), Permissions: list.AclPermissionsWriter})
			}
		}
		if err = space.AclClient().AddAccounts(ctx, list.AccountsAddPayload{Additions: additions}); err != nil {
			return provision, err
		}
	}
	root, err := space.TreeBuilder().CreateTree(ctx, objecttree.ObjectTreeCreatePayload{PrivKey: owner, ChangeType: config.Namespace, SpaceId: space.Id(), IsEncrypted: true, Seed: []byte(core.RandomID()), Timestamp: time.Now().Unix()})
	if err != nil {
		return provision, err
	}
	space.Acl().RLock()
	acl, err := space.Acl().RecordsAfter(ctx, "")
	space.Acl().RUnlock()
	if err != nil {
		return provision, err
	}
	replay, err := core.ReplayMigration(ctx, source, root.RootRawChange, acl, networkID, signers)
	if err != nil {
		return provision, err
	}
	tree, err := host.PutTree(ctx, space.Id(), replay.Payload, nil)
	if err != nil {
		return provision, err
	}
	if err = space.SyncHeads(ctx); err != nil {
		return provision, err
	}
	signed, err := privateclient.SignConnection(privateclient.Connection{Version: 1, Namespace: config.Namespace, NetworkID: networkID, SpaceID: space.Id(), TreeID: tree.Id(), Owner: owner.GetPublic().Account(), Members: bundles[0].Members}, owner)
	if err != nil {
		return provision, err
	}
	provision = Provision{NetworkConfig: network, NetworkID: networkID, Owner: owner.GetPublic().Account(), Workspace: signed}
	other, err := accountdata.NewRandom()
	if err != nil {
		return Provision{}, err
	}
	verifier, err := privateclient.Open(ctx, filepath.Join(destination, "verification"), config, network, networkID, provision.Owner, accountdata.New(other.PeerKey, owner), signed)
	if err != nil {
		return Provision{}, err
	}
	err = verifier.VerifyLegacy(source)
	closeCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	err = errors.Join(err, verifier.Close(closeCtx))
	if err != nil {
		return Provision{}, err
	}
	if err = core.AtomicJSON(filepath.Join(destination, "provision.json"), provision); err != nil {
		return Provision{}, err
	}
	if err = core.AtomicJSON(filepath.Join(destination, "change-map.json"), replay.Changes); err != nil {
		return Provision{}, err
	}
	return provision, nil
}
