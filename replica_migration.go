package synccore

// VerifyLegacy requires every legacy operation, including superseded versions,
// to exist unchanged with its original author in the native tree. It does not
// clear pending writes or treat a network broadcast as a delivery receipt.
func (r *Replica) VerifyLegacy(source *Engine) error {
	old, authors, err := source.Operations()
	if err != nil {
		return err
	}
	r.tree.Lock()
	defer r.tree.Unlock()
	current, currentAuthors, err := operationsLocked(r.config, r.tree, r.members)
	if err != nil {
		return err
	}
	for id, operation := range old {
		found, ok := current[id]
		if !ok || !SameOperation(operation, found) || authors[id] != currentAuthors[id] {
			return NewError("sync_incomplete", nil)
		}
	}
	return nil
}
