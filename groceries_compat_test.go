package synccore

import (
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

var fields = map[string]string{
	"product": "name category notes archived", "category": "name archived", "city": "name archived",
	"shop": "name city notes archived", "price": "product shop package size sizeUnit amount unitAmount unit currency notes archived",
	"list": "name notes archived", "entry": "list product quantity unit notes shop price done archived sessionDone",
	"history": "data", "historySettings": "enabled retentionDays sessionLimit clearedAt",
}
var decimal = regexp.MustCompile(`^(0|[1-9][0-9]{0,8})(\.[0-9]{1,4})?$`)

func ValidateOperation(op Operation) error {
	if s := op.Source; s != nil {
		if len(s.ID) < 16 || len(s.ID) > 128 ||
			(s.Platform != "android" && s.Platform != "web") ||
			strings.TrimSpace(s.Name) == "" || !utf8.ValidString(s.Name) || utf8.RuneCountInString(s.Name) > 64 ||
			strings.IndexFunc(s.Name, unicode.IsControl) >= 0 {
			return errors.New("invalid device source")
		}
	}
	if len(op.ID) < 16 || len(op.ID) > 128 || len(op.Record) < 16 || len(op.Record) > 128 || len(op.Replaces) > 1000 {
		return errors.New("invalid identifier")
	}
	if !strings.Contains(" "+fields[op.Kind]+" ", " "+op.Field+" ") || op.Field == "" || fields[op.Kind] == "" {
		return errors.New("unknown record kind or field")
	}
	if len(op.Value) > 16000 {
		return errors.New("value too long")
	}
	if op.Kind == "history" && op.Value != "" && !json.Valid([]byte(op.Value)) {
		return errors.New("invalid history data")
	}
	if op.Kind == "historySettings" {
		switch op.Field {
		case "enabled":
			if op.Value != "true" && op.Value != "false" {
				return errors.New("expected boolean")
			}
		case "sessionLimit":
			n, err := strconv.Atoi(op.Value)
			if err != nil || n < 1 || n > 100000 {
				return errors.New("invalid history session limit")
			}
		case "retentionDays":
			n, err := strconv.Atoi(op.Value)
			if err != nil || n < 1 || n > 36500 {
				return errors.New("invalid history retention")
			}
		case "clearedAt":
			n, err := strconv.ParseInt(op.Value, 10, 64)
			if err != nil || n < 0 {
				return errors.New("invalid history date")
			}
		}
	}
	if op.Field == "name" && (strings.TrimSpace(op.Value) == "" || len(op.Value) > 500) {
		return errors.New("name required (maximum 500 bytes)")
	}
	if op.Field == "archived" || op.Field == "done" {
		if op.Value != "true" && op.Value != "false" {
			return errors.New("expected boolean")
		}
	}
	if op.Field == "quantity" || op.Field == "size" || op.Field == "amount" || op.Field == "unitAmount" {
		if op.Value != "" && !decimal.MatchString(op.Value) {
			return errors.New("expected positive decimal, maximum four decimal places")
		}
	}
	if op.Field == "quantity" || op.Field == "size" {
		if op.Value != "" {
			n, _ := new(big.Rat).SetString(op.Value)
			if n.Sign() <= 0 {
				return errors.New("quantity must be greater than zero")
			}
		}
	}
	if op.Field == "unit" || op.Field == "sizeUnit" {
		if !strings.Contains("|item|pack|kg|g|L|ml|", "|"+op.Value+"|") {
			return errors.New("invalid unit")
		}
	}
	if op.Field == "currency" && op.Value != "EUR" {
		return errors.New("currency must be EUR")
	}
	return nil
}
func ValidateRecords(v View) error {
	all := map[string]Record{}
	for _, r := range v.Records {
		all[r.ID] = r
	}
	refs := map[string]string{"category": "category", "city": "city", "shop": "shop", "product": "product", "list": "list", "price": "price"}
	for _, r := range v.Records {
		required := []string{"name"}
		switch r.Kind {
		case "history", "historySettings":
			required = nil
		case "entry":
			required = []string{"list", "product", "quantity", "unit", "done"}
		case "price":
			required = []string{"product", "currency"}
		}
		for _, f := range required {
			if r.Value(f) == "" {
				return fmt.Errorf("%s requires %s", r.Kind, f)
			}
		}
		for field, kind := range refs {
			for _, v := range r.Fields[field] {
				if v.Value != "" && all[v.Value].Kind != kind {
					return fmt.Errorf("invalid %s reference", field)
				}
			}
		}
	}
	return nil
}

var testConfig = Config{Namespace: "studio.camille.groceries.operation.v1", Platforms: []string{"android", "web"}, ValidateOperation: ValidateOperation, ValidateRecords: ValidateRecords}
