package synccore

import (
	"encoding/json"
	"sort"
)

// Source is a snapshot of the originating installation at the time of an edit.
type Source struct {
	ID       string `json:"id"`
	Name     string `json:"name"`
	Platform string `json:"platform"`
}

type Operation struct {
	ID       string   `json:"id"`
	Record   string   `json:"record"`
	Kind     string   `json:"kind"`
	Field    string   `json:"field"`
	Value    string   `json:"value"`
	Replaces []string `json:"replaces"`
	Source   *Source  `json:"source,omitempty"`
}
type Version struct {
	ID     string  `json:"id"`
	Value  string  `json:"value"`
	Author string  `json:"author"`
	Source *Source `json:"source,omitempty"`
}
type Record struct {
	ID     string               `json:"id"`
	Kind   string               `json:"kind"`
	Fields map[string][]Version `json:"fields"`
}
type View struct {
	Records []Record `json:"records"`
	Changes int      `json:"changes"`
	User    string   `json:"user"`
}

func Materialize(ops map[string]Operation, authors map[string]string, user string) View {
	superseded := map[string]bool{}
	records := map[string]*Record{}
	for _, o := range ops {
		for _, id := range o.Replaces {
			superseded[id] = true
		}
		if records[o.Record] == nil {
			records[o.Record] = &Record{ID: o.Record, Kind: o.Kind, Fields: map[string][]Version{}}
		}
	}
	for id, o := range ops {
		if !superseded[id] {
			r := records[o.Record]
			source := o.Source
			if source != nil {
				copy := *source
				source = &copy
			}
			r.Fields[o.Field] = append(r.Fields[o.Field], Version{ID: id, Value: o.Value, Author: authors[id], Source: source})
		}
	}
	v := View{Records: []Record{}, Changes: len(ops), User: user}
	for _, r := range records {
		for _, vs := range r.Fields {
			sort.Slice(vs, func(i, j int) bool { return vs[i].ID < vs[j].ID })
		}
		v.Records = append(v.Records, *r)
	}
	sort.Slice(v.Records, func(i, j int) bool { return v.Records[i].ID < v.Records[j].ID })
	return v
}
func (r Record) Value(field string) string {
	vs := r.Fields[field]
	if len(vs) == 0 {
		return ""
	}
	return vs[0].Value
}
func (r Record) Conflict(field string) bool {
	seen := map[string]bool{}
	for _, v := range r.Fields[field] {
		seen[v.Value] = true
	}
	return len(seen) > 1
}
func SameOperation(a, b Operation) bool {
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}
