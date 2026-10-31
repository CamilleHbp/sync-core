package synccore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/anyproto/any-sync/commonspace/object/tree/objecttree"
	"github.com/anyproto/any-sync/commonspace/object/tree/treechangeproto"
	_ "modernc.org/sqlite"
	"os"
	"path/filepath"
)

type Database struct{ SQL *sql.DB }

func OpenDatabase(path string) (*Database, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA busy_timeout=5000;
 CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS changes(id TEXT PRIMARY KEY, seq INTEGER UNIQUE NOT NULL, value BLOB NOT NULL);
 CREATE TABLE IF NOT EXISTS records(id TEXT PRIMARY KEY, seq INTEGER NOT NULL, value BLOB NOT NULL);
 CREATE INDEX IF NOT EXISTS records_seq ON records(seq);
 CREATE TABLE IF NOT EXISTS outbox(id TEXT PRIMARY KEY);
 PRAGMA user_version=1;`)
	if err != nil {
		db.Close()
		return nil, err
	}
	if err = os.Chmod(path, 0600); err != nil {
		db.Close()
		return nil, err
	}
	return &Database{db}, nil
}
func (d *Database) Close() error { return d.SQL.Close() }
func (d *Database) Get(key string, v any) error {
	var b []byte
	err := d.SQL.QueryRow("SELECT value FROM meta WHERE key=?", key).Scan(&b)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, v)
}
func (d *Database) Put(key string, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	_, err = d.SQL.Exec("INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", key, b)
	return err
}
func (d *Database) Load() (*MemoryStorage, error) {
	var st MemoryStorage
	if err := d.Get("tree", &st); err != nil {
		return nil, err
	}
	st.Changes = map[string]objecttree.StorageChange{}
	rows, err := d.SQL.Query("SELECT value FROM changes ORDER BY seq")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var b []byte
		var ch objecttree.StorageChange
		if err = rows.Scan(&b); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(b, &ch); err != nil {
			return nil, err
		}
		st.Changes[ch.Id] = ch
	}
	return &st, rows.Err()
}

// Commit saves signed changes, projections, progress and outbox atomically.
func (d *Database) Commit(e *Engine, meta map[string]any, local bool, ack []string) error {
	v, err := e.View()
	if err != nil {
		return err
	}
	tx, err := d.SQL.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var seq uint64
	if err = tx.QueryRow("SELECT COALESCE(MAX(seq),0) FROM changes").Scan(&seq); err != nil {
		return err
	}
	dirty := map[string]bool{}
	writeAll := seq == 0
	for _, c := range e.Store.Changes {
		if c.AddSeq > seq || c.Id == e.Store.RootID {
			if c.Id != e.Store.RootID {
				change, getErr := e.Tree.GetChange(c.Id)
				if getErr != nil {
					return getErr
				}
				if operations, ok := change.Model.([]Operation); ok {
					for _, operation := range operations {
						dirty[operation.Record] = true
					}
				} else {
					writeAll = true
				}
			}
			b, _ := json.Marshal(c)
			if _, err = tx.Exec("INSERT OR IGNORE INTO changes VALUES(?,?,?)", c.Id, c.AddSeq, b); err != nil {
				return err
			}
			if local && c.AddSeq > seq {
				if _, err = tx.Exec("INSERT OR IGNORE INTO outbox VALUES(?)", c.Id); err != nil {
					return err
				}
			}
		}
	}
	for _, r := range v.Records {
		if !writeAll && !dirty[r.ID] {
			continue
		}
		b, _ := json.Marshal(r)
		if _, err = tx.Exec(`INSERT INTO records VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET seq=excluded.seq,value=excluded.value WHERE records.value!=excluded.value`, r.ID, e.Store.Seq, b); err != nil {
			return err
		}
	}
	st := *e.Store
	st.Changes = nil
	if meta == nil {
		meta = map[string]any{}
	}
	meta["tree"] = st
	for k, val := range meta {
		b, er := json.Marshal(val)
		if er != nil {
			return er
		}
		if _, err = tx.Exec("INSERT INTO meta VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", k, b); err != nil {
			return err
		}
	}
	for _, id := range ack {
		if _, err = tx.Exec("DELETE FROM outbox WHERE id=?", id); err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (d *Database) Pending() ([]string, error) {
	rows, err := d.SQL.Query("SELECT o.id FROM outbox o JOIN changes c ON c.id=o.id ORDER BY c.seq LIMIT ?", MaxBatch)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	ids := []string{}
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}
func (d *Database) PendingCount() int {
	var n int
	d.SQL.QueryRow("SELECT COUNT(*) FROM outbox").Scan(&n)
	return n
}

type RecordPage struct {
	Records []Record `json:"records"`
	Cursor  uint64   `json:"cursor"`
	More    bool     `json:"more"`
}

func (d *Database) Records(after uint64) (RecordPage, error) {
	p := RecordPage{Records: []Record{}, Cursor: after}
	tx, err := d.SQL.BeginTx(context.Background(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return p, err
	}
	defer tx.Rollback()
	var edge sql.NullInt64
	err = tx.QueryRow("SELECT MAX(seq) FROM (SELECT DISTINCT seq FROM records WHERE seq>? ORDER BY seq LIMIT 1)", after).Scan(&edge)
	if err != nil {
		return p, err
	}
	if !edge.Valid {
		return p, tx.Commit()
	}
	rows, err := tx.Query("SELECT value,seq FROM records WHERE seq>? AND seq<=? ORDER BY seq,id", after, edge.Int64)
	if err != nil {
		return p, err
	}
	var values [][]byte
	for rows.Next() {
		var value []byte
		if err = rows.Scan(&value, &p.Cursor); err != nil {
			rows.Close()
			return p, err
		}
		values = append(values, value)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return p, err
	}
	if err = tx.QueryRow("SELECT EXISTS(SELECT 1 FROM records WHERE seq>?)", p.Cursor).Scan(&p.More); err != nil {
		return p, err
	}
	if err = tx.Commit(); err != nil {
		return p, err
	}
	// Release the database connection before decoding an owned committed page.
	for _, value := range values {
		var record Record
		if err = json.Unmarshal(value, &record); err != nil {
			return p, err
		}
		p.Records = append(p.Records, record)
	}
	return p, nil
}

type SyncRequest struct {
	Packet Packet   `json:"packet"`
	Cursor uint64   `json:"cursor"`
	Need   []string `json:"need"`
}
type SyncResponse struct {
	Packet Packet   `json:"packet"`
	Cursor uint64   `json:"cursor"`
	More   bool     `json:"more"`
	Ack    []string `json:"ack"`
	Need   []string `json:"need"`
}

func (e *Engine) Page(after uint64) SyncResponse {
	r := SyncResponse{Packet: Packet{RootID: e.Store.RootID, Changes: []*treechangeproto.RawTreeChangeWithId{}, Heads: []string{}}, Cursor: after, Ack: []string{}, Need: []string{}}
	bytes := 0
	e.Store.GetAfterAddSeq(context.Background(), after, func(_ context.Context, c objecttree.StorageChange) (bool, error) {
		if len(r.Packet.Changes) >= MaxBatch || (bytes+len(c.RawChange) > 1048576 && len(r.Packet.Changes) > 0) {
			r.More = true
			return false, nil
		}
		bytes += len(c.RawChange)
		r.Packet.Changes = append(r.Packet.Changes, c.RawTreeChangeWithId())
		r.Packet.Heads = append(r.Packet.Heads, c.Id)
		r.Cursor = c.AddSeq
		return true, nil
	})
	return r
}
func (e *Engine) PacketIDs(ids []string) (Packet, error) {
	p := Packet{RootID: e.Store.RootID, Heads: []string{}, Changes: []*treechangeproto.RawTreeChangeWithId{}}
	if len(ids) > MaxBatch {
		return p, errors.New("batch too large")
	}
	for _, id := range ids {
		c, ok := e.Store.Changes[id]
		if !ok {
			return p, errors.New("missing change")
		}
		p.Changes = append(p.Changes, c.RawTreeChangeWithId())
		p.Heads = append(p.Heads, id)
	}
	return p, nil
}
