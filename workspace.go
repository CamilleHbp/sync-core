package synccore

import (
	"github.com/anyproto/any-sync/commonspace/object/accountdata"
	"github.com/anyproto/any-sync/commonspace/object/acl/list"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/consensus/consensusproto"
	"github.com/anyproto/any-sync/util/cidutil"
	"github.com/anyproto/any-sync/util/crypto"
	"strings"
)

// NewWorkspace creates a private workspace or an explicitly supplied initial
// membership. The first account owns it; subsequent accounts have writer access.
// Account bundles contain private keys and must be returned only to their owner.
func NewWorkspace(config Config, names []string) ([]Bundle, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}
	if len(names) == 0 || len(names) > 100 {
		return nil, NewError("invalid_membership", nil)
	}
	seen := map[string]bool{}
	keys := []*accountdata.AccountKeys{}
	for _, name := range names {
		if strings.TrimSpace(name) == "" || seen[name] {
			return nil, NewError("invalid_membership", nil)
		}
		seen[name] = true
		k, err := accountdata.NewRandom()
		if err != nil {
			return nil, err
		}
		keys = append(keys, k)
	}
	space, err := cidutil.NewCidFromBytes([]byte(RandomID()))
	if err != nil {
		return nil, err
	}
	acl, err := list.NewInMemoryDerivedAcl(space, keys[0])
	if err != nil {
		return nil, err
	}
	records := []*consensusproto.RawRecordWithId{acl.Root()}
	if len(keys) > 1 {
		additions := []list.AccountAdd{}
		for i, k := range keys[1:] {
			additions = append(additions, list.AccountAdd{Identity: k.SignKey.GetPublic(), Permissions: list.AclPermissionsWriter, Metadata: []byte(names[i+1])})
		}
		record, err := acl.RecordBuilder().BuildAccountsAdd(list.AccountsAddPayload{Additions: additions})
		if err != nil {
			return nil, err
		}
		payload, err := record.MarshalVT()
		if err != nil {
			return nil, err
		}
		id, err := cidutil.NewCidFromBytes(payload)
		if err != nil {
			return nil, err
		}
		raw := &consensusproto.RawRecordWithId{Id: id, Payload: payload}
		if err = acl.AddRawRecord(raw); err != nil {
			return nil, err
		}
		records = append(records, raw)
	}
	builder := objecttree.NewChangeBuilder(crypto.NewKeyStorage(), nil)
	_, root, err := builder.BuildRoot(objecttree.InitialContent{AclHeadId: acl.Head().Id, PrivKey: keys[0].SignKey, SpaceId: space, Seed: []byte(RandomID()), ChangeType: config.Namespace})
	if err != nil {
		return nil, err
	}
	members := map[string]string{}
	for i, k := range keys {
		members[k.SignKey.GetPublic().Account()] = names[i]
	}
	bundles := []Bundle{}
	for i, k := range keys {
		raw, err := k.SignKey.Marshall()
		if err != nil {
			return nil, err
		}
		bundles = append(bundles, Bundle{User: names[i], Key: raw, ACL: records, Root: root, Members: members})
	}
	return bundles, nil
}
