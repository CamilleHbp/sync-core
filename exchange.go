package synccore

// Exchange serves one authorized workspace. The application server must select
// the authorized workspace and serialize calls for it. It owns access codes and
// sessions; the library never chooses an account on behalf of the application.
func Exchange(engine *Engine, db *Database, request SyncRequest) (*Engine, SyncResponse, error) {
	fail := func(err error) (*Engine, SyncResponse, error) { return engine, SyncResponse{}, err }
	if len(request.Need) > MaxBatch {
		return fail(NewError("invalid_request", nil))
	}
	if len(request.Need) > 0 {
		packet, err := engine.PacketIDs(request.Need)
		if err != nil {
			return fail(err)
		}
		return engine, SyncResponse{Packet: packet, Cursor: request.Cursor, More: true}, nil
	}
	next, err := engine.Clone()
	if err != nil {
		return fail(err)
	}
	need, err := next.Missing(request.Packet)
	if err != nil {
		return fail(err)
	}
	if len(need) > 0 {
		return engine, SyncResponse{Need: need, Cursor: request.Cursor}, nil
	}
	if err = next.Merge(request.Packet, ""); err != nil {
		return fail(err)
	}
	if err = db.Commit(next, nil, false, nil); err != nil {
		return fail(err)
	}
	page := next.Page(request.Cursor)
	for _, change := range request.Packet.Changes {
		page.Ack = append(page.Ack, change.Id)
	}
	return next, page, nil
}
