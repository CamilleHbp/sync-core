package synccore

import (
	"errors"
	"strings"
	"unicode"
	"unicode/utf8"
)

// Config belongs to one application schema. Callbacks must be deterministic and
// accept every historical schema version that this application still supports.
type Config struct {
	Namespace         string
	Platforms         []string
	ValidateOperation func(Operation) error
	ValidateRecords   func(View) error
}

func (c Config) validate() error {
	if strings.TrimSpace(c.Namespace) == "" || c.ValidateOperation == nil {
		return NewError("invalid_config", nil)
	}
	return nil
}
func (c Config) validateOperation(op Operation) error {
	if len(op.ID) < 16 || len(op.ID) > 128 || len(op.Record) < 16 || len(op.Record) > 128 || len(op.Replaces) > 1000 {
		return NewError("invalid_operation", nil)
	}
	if len(op.Value) > 128000 || !utf8.ValidString(op.Value) {
		return NewError("invalid_value", nil)
	}
	if s := op.Source; s != nil {
		allowed := false
		for _, p := range c.Platforms {
			if p == s.Platform {
				allowed = true
			}
		}
		if !allowed || len(s.ID) < 16 || len(s.ID) > 128 || strings.TrimSpace(s.Name) == "" || !utf8.ValidString(s.Name) || utf8.RuneCountInString(s.Name) > 64 || strings.IndexFunc(s.Name, unicode.IsControl) >= 0 {
			return NewError("invalid_source", nil)
		}
	}
	return c.ValidateOperation(op)
}
func (c Config) validateRecords(v View) error {
	if c.ValidateRecords != nil {
		return c.ValidateRecords(v)
	}
	return nil
}

// Error has a stable machine code. Applications localize codes at their UI boundary.
type Error struct {
	Code  string
	Cause error
}

func NewError(code string, cause error) *Error { return &Error{code, cause} }
func (e *Error) Error() string {
	if e.Cause != nil {
		return e.Code + ": " + e.Cause.Error()
	}
	return e.Code
}
func (e *Error) Unwrap() error { return e.Cause }
func ErrorCode(err error) string {
	var e *Error
	if errors.As(err, &e) {
		return e.Code
	}
	if err == nil {
		return ""
	}
	return "sync_failed"
}
