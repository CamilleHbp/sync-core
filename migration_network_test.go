package synccore

import (
	"bytes"
	"errors"
	"testing"

	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/acl/recordverifier"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/util/cidutil"
	"github.com/anyproto/any-sync/util/crypto"
)

func TestLegacyMembershipCannotBeSilentlyActivatedOnOfficialNetwork(t *testing.T) {
	_, bundles, _ := seed(t)
	_, networkKey, err := crypto.GenerateRandomEd25519KeyPair()
	must(t, err)
	for _, bundle := range bundles {
		err = ValidateMigrationACL(testConfig, bundle, networkKey.Network())
		if !errors.Is(err, ErrMigrationACLUnaccepted) {
			t.Fatalf("expected network admission failure, got %v", err)
		}
		t.Logf("network-root verification rejects legacy membership: %v", err)
	}
	if err = ValidateMigrationACL(testConfig, bundles[0], ""); err == nil {
		t.Fatal("empty network silently bypassed verification")
	}
}

func TestNetworkAcceptanceChangesLegacyACLIdentity(t *testing.T) {
	// All keys are synthetic. Demonstrate why adding acceptor fields is not a
	// lossless migration: ACL IDs hash the entire signed envelope, not its body.
	_, bundles, _ := seed(t)
	original := bundles[0].ACL[1]
	root, err := objecttree.UnmarshallRoot(bundles[0].Root)
	must(t, err)
	if root.AclHeadId != original.Id {
		t.Fatal("fixture must reference legacy membership")
	}
	var record consensusproto.RawRecord
	must(t, record.UnmarshalVT(original.Payload))
	networkSigner, networkKey, err := crypto.GenerateRandomEd25519KeyPair()
	must(t, err)
	record.AcceptorIdentity, err = networkKey.Marshall()
	must(t, err)
	record.AcceptorSignature, err = networkSigner.Sign(record.Payload)
	must(t, err)
	must(t, recordverifier.New(networkKey).VerifyAcceptor(&record))
	accepted, err := record.MarshalVT()
	must(t, err)
	acceptedID, err := cidutil.NewCidFromBytes(accepted)
	must(t, err)
	if bytes.Equal(accepted, original.Payload) || acceptedID == original.Id {
		t.Fatal("network acceptance should change the CID")
	}
	// Pretending the accepted envelope still has its old ID fails upstream's
	// content-address validation, even with a valid network-root signature.
	storage, err := list.NewInMemoryStorage(bundles[0].ACL[0].Id, []*consensusproto.RawRecordWithId{
		bundles[0].ACL[0], {Id: original.Id, Payload: accepted},
	})
	must(t, err)
	engine, err := Open(testConfig, bundles[0], nil)
	must(t, err)
	_, err = list.BuildAclListWithIdentity(engine.Keys, storage, recordverifier.New(networkKey))
	if !errors.Is(err, list.ErrIncorrectCID) {
		t.Fatalf("expected changed CID rejection, got %v", err)
	}
}
