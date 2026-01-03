// Package document provides document-related types and validation for Vex.
package document

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// MaxStringIDBytes is the maximum size for string document IDs.
const MaxStringIDBytes = 64

// IDType represents the type of a document ID.
type IDType int

const (
	IDTypeU64 IDType = iota
	IDTypeUUID
	IDTypeString
)

func (t IDType) String() string {
	switch t {
	case IDTypeU64:
		return "u64"
	case IDTypeUUID:
		return "uuid"
	case IDTypeString:
		return "string"
	default:
		return "unknown"
	}
}

// ID represents a normalized document ID.
// Document IDs can be u64, UUID, or strings up to 64 bytes.
type ID struct {
	typ    IDType
	u64    uint64
	uuid   uuid.UUID
	str    string
}

// Type returns the type of this ID.
func (id ID) Type() IDType {
	return id.typ
}

// U64 returns the u64 value if this is a u64 ID, or 0 otherwise.
func (id ID) U64() uint64 {
	return id.u64
}

// UUID returns the UUID value if this is a UUID ID, or empty otherwise.
func (id ID) UUID() uuid.UUID {
	return id.uuid
}

// String returns the string representation of this ID.
func (id ID) String() string {
	switch id.typ {
	case IDTypeU64:
		return strconv.FormatUint(id.u64, 10)
	case IDTypeUUID:
		return id.uuid.String()
	case IDTypeString:
		return id.str
	default:
		return ""
	}
}

// NewU64ID creates an ID from a uint64 value.
func NewU64ID(v uint64) ID {
	return ID{typ: IDTypeU64, u64: v}
}

// NewUUIDID creates an ID from a UUID value.
func NewUUIDID(v uuid.UUID) ID {
	return ID{typ: IDTypeUUID, uuid: v}
}

// NewStringID creates an ID from a string value.
// Returns an error if the string exceeds MaxStringIDBytes.
func NewStringID(v string) (ID, error) {
	if len(v) > MaxStringIDBytes {
		return ID{}, &ValidationError{
			Field:   "id",
			Message: fmt.Sprintf("string ID exceeds maximum length of %d bytes (got %d bytes)", MaxStringIDBytes, len(v)),
		}
	}
	return ID{typ: IDTypeString, str: v}, nil
}

// ValidationError represents a validation error with field context.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// ParseID parses and normalizes a document ID from any supported input type.
// Supported input types:
//   - uint64, int64, int, uint: parsed as u64
//   - string: checked for UUID format, otherwise treated as string
//   - json.Number: parsed as u64 or rejected if not valid u64
//   - uuid.UUID: used directly
//   - float64: converted to u64 if it represents a whole number
//
// Returns an error if:
//   - String ID exceeds 64 bytes
//   - Numeric value is negative or not a valid u64
//   - Input type is not supported
func ParseID(v any) (ID, error) {
	switch val := v.(type) {
	case uint64:
		return NewU64ID(val), nil

	case int64:
		if val < 0 {
			return ID{}, &ValidationError{
				Field:   "id",
				Message: "negative integer IDs are not allowed",
			}
		}
		return NewU64ID(uint64(val)), nil

	case int:
		if val < 0 {
			return ID{}, &ValidationError{
				Field:   "id",
				Message: "negative integer IDs are not allowed",
			}
		}
		return NewU64ID(uint64(val)), nil

	case uint:
		return NewU64ID(uint64(val)), nil

	case float64:
		maxUint64 := float64(^uint64(0))
		if val < 0 || val > maxUint64 || val != float64(uint64(val)) {
			return ID{}, &ValidationError{
				Field:   "id",
				Message: "numeric ID must be a non-negative integer",
			}
		}
		return NewU64ID(uint64(val)), nil

	case json.Number:
		u, err := strconv.ParseUint(val.String(), 10, 64)
		if err != nil {
			return ID{}, &ValidationError{
				Field:   "id",
				Message: fmt.Sprintf("invalid numeric ID: %v", err),
			}
		}
		return NewU64ID(uint64(u)), nil

	case uuid.UUID:
		return NewUUIDID(val), nil

	case string:
		return parseStringID(val)

	default:
		return ID{}, &ValidationError{
			Field:   "id",
			Message: fmt.Sprintf("unsupported ID type: %T", v),
		}
	}
}

// ParseIDKey parses a document ID encoded with a type prefix (u64:, uuid:, str:).
// This format is used internally to preserve ID types in serialized artifacts.
func ParseIDKey(s string) (ID, error) {
	if strings.HasPrefix(s, "u64:") {
		rest := strings.TrimPrefix(s, "u64:")
		if rest == "" {
			return ID{}, &ValidationError{Field: "id", Message: "empty u64 ID"}
		}
		u, err := strconv.ParseUint(rest, 10, 64)
		if err != nil {
			return ID{}, &ValidationError{Field: "id", Message: fmt.Sprintf("invalid u64 ID: %v", err)}
		}
		return NewU64ID(u), nil
	}

	if strings.HasPrefix(s, "uuid:") {
		rest := strings.TrimPrefix(s, "uuid:")
		if len(rest) == 0 {
			return ID{}, &ValidationError{Field: "id", Message: "empty uuid ID"}
		}
		decoded, err := hex.DecodeString(rest)
		if err != nil || len(decoded) != 16 {
			return ID{}, &ValidationError{Field: "id", Message: "invalid uuid ID"}
		}
		var u uuid.UUID
		copy(u[:], decoded)
		return NewUUIDID(u), nil
	}

	if strings.HasPrefix(s, "str:") {
		rest := strings.TrimPrefix(s, "str:")
		return NewStringID(rest)
	}

	return ID{}, &ValidationError{Field: "id", Message: "unsupported ID key format"}
}

// parseStringID parses a string value and normalizes it to the appropriate ID type.
// It tries to parse as UUID first, and falls back to string.
func parseStringID(s string) (ID, error) {
	if len(s) == 0 {
		return ID{}, &ValidationError{
			Field:   "id",
			Message: "empty ID is not allowed",
		}
	}

	if len(s) > MaxStringIDBytes {
		return ID{}, &ValidationError{
			Field:   "id",
			Message: fmt.Sprintf("string ID exceeds maximum length of %d bytes (got %d bytes)", MaxStringIDBytes, len(s)),
		}
	}

	// Try to parse as UUID (standard format with dashes)
	if len(s) == 36 && strings.Count(s, "-") == 4 {
		if u, err := uuid.Parse(s); err == nil {
			return NewUUIDID(u), nil
		}
	}

	// Otherwise, treat as string ID
	return NewStringID(s)
}

// MarshalJSON implements json.Marshaler for ID.
func (id ID) MarshalJSON() ([]byte, error) {
	switch id.typ {
	case IDTypeU64:
		return json.Marshal(id.u64)
	case IDTypeUUID:
		return json.Marshal(id.uuid.String())
	case IDTypeString:
		return json.Marshal(id.str)
	default:
		return nil, fmt.Errorf("unknown ID type: %d", id.typ)
	}
}

// UnmarshalJSON implements json.Unmarshaler for ID.
func (id *ID) UnmarshalJSON(data []byte) error {
	var raw any
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	parsed, err := ParseID(raw)
	if err != nil {
		return err
	}
	*id = parsed
	return nil
}

// Equal returns true if two IDs are equal.
func (id ID) Equal(other ID) bool {
	if id.typ != other.typ {
		return false
	}
	switch id.typ {
	case IDTypeU64:
		return id.u64 == other.u64
	case IDTypeUUID:
		return id.uuid == other.uuid
	case IDTypeString:
		return id.str == other.str
	default:
		return false
	}
}

// Compare compares two IDs for ordering.
// Returns -1 if id < other, 0 if id == other, 1 if id > other.
// IDs of different types are ordered by type: u64 < uuid < string.
func (id ID) Compare(other ID) int {
	if id.typ != other.typ {
		if id.typ < other.typ {
			return -1
		}
		return 1
	}
	switch id.typ {
	case IDTypeU64:
		if id.u64 < other.u64 {
			return -1
		}
		if id.u64 > other.u64 {
			return 1
		}
		return 0
	case IDTypeUUID:
		idBytes := id.uuid[:]
		otherBytes := other.uuid[:]
		for i := 0; i < 16; i++ {
			if idBytes[i] < otherBytes[i] {
				return -1
			}
			if idBytes[i] > otherBytes[i] {
				return 1
			}
		}
		return 0
	case IDTypeString:
		if id.str < other.str {
			return -1
		}
		if id.str > other.str {
			return 1
		}
		return 0
	default:
		return 0
	}
}
