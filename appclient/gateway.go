package appclient

import (
	"context"
	"encoding/json"
	core "github.com/CamilleHbp/sync-core"
	"github.com/CamilleHbp/sync-core/privateclient"
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/util/crypto"
	"os"
	"path/filepath"
)

// OpenGateway binds the existing trusted HTTPS gateway to its private-network
// tree. The same public provisioning file is served only after authentication.
// It never creates a replacement workspace or silently migrates production.
func OpenGateway(ctx context.Context, directory, provisionPath string, config core.Config, bundle core.Bundle) (*privateclient.Client, error) {
	raw, err := os.ReadFile(provisionPath)
	if err != nil {
		return nil, err
	}
	var p Provision
	if err = json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	key, err := crypto.UnmarshalEd25519PrivateKeyProto(bundle.Key)
	if err != nil {
		return nil, err
	}
	peer, err := accountdata.NewRandom()
	if err != nil {
		return nil, err
	}
	return privateclient.Open(ctx, filepath.Join(directory, "native-gateway"), config, p.NetworkConfig, p.NetworkID, p.Owner, accountdata.New(peer.PeerKey, key), p.Workspace)
}
