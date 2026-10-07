package synccore

import (
	"errors"
	"fmt"

	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/util/crypto"
)

// ErrMigrationACLUnaccepted means the existing ACL cannot be replayed using
// the target network's trust anchor. Do not bypass verification to activate it.
var ErrMigrationACLUnaccepted = errors.New("migration ACL is not accepted by the target network")

// ValidateMigrationACL checks existing membership records using the official
// network-root verifier, after full local validation. This is a necessary check,
// NOT coordinator admission or proof of full network compatibility. An empty
// network ID is rejected rather than silently selecting local-only verification.
func ValidateMigrationACL(config Config, bundle Bundle, networkID string) error {
	networkKey, err := crypto.DecodeNetworkId(networkID)
	if err != nil {
		return fmt.Errorf("invalid migration network ID: %w", err)
	}
	engine, err := Open(config, bundle, nil)
	if err != nil {
		return err
	}
	storage, err := list.NewInMemoryStorage(bundle.ACL[0].Id, bundle.ACL)
	if err != nil {
		return err
	}
	_, err = list.BuildAclListWithIdentity(engine.Keys, storage, recordverifier.New(networkKey))
	if err != nil {
		return fmt.Errorf("%w: %w", ErrMigrationACLUnaccepted, err)
	}
	return nil
}
