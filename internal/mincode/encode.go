package mincode

import (
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
)

// Encode is the inverse of Decode - mainly useful for tests and dev tooling
// that need to construct a realistic mincode-encoded "u" query param (e.g.
// simulating what a real divolte.js client would send). Accepts the same
// generic shapes Decode produces: map[string]interface{}, []interface{},
// string, bool, nil, and any integer/float Go type.
//
// Map key iteration order is sorted for deterministic output (mincode
// itself has no notion of field order beyond what's written, so this is
// just for reproducible tests/tooling, not a wire requirement).
func Encode(v interface{}) (string, error) {
	var sb strings.Builder
	if err := encodeValue(&sb, v, true); err != nil {
		return "", err
	}
	return sb.String(), nil
}

// encodeValue writes v's encoding to sb. withTag controls whether the
// leading type-tag character is written - false is used when encoding a
// field's value where the field-name-scan already consumed the tag
// character (mincode's "tag doubles as the next value's type" trick).
func encodeValue(sb *strings.Builder, v interface{}, withTag bool) error {
	switch val := v.(type) {
	case nil:
		if withTag {
			sb.WriteByte('n')
		}
		return nil
	case bool:
		if withTag {
			if val {
				sb.WriteByte('t')
			} else {
				sb.WriteByte('f')
			}
		}
		return nil
	case string:
		if withTag {
			sb.WriteByte('s')
		}
		writeEscaped(sb, val)
		sb.WriteByte('!')
		return nil
	case map[string]interface{}:
		if withTag {
			sb.WriteByte('(')
		}
		keys := make([]string, 0, len(val))
		for k := range val {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			fv := val[k]
			tag, err := tagFor(fv)
			if err != nil {
				return fmt.Errorf("field %q: %w", k, err)
			}
			sb.WriteByte(tag)
			writeEscaped(sb, k)
			sb.WriteByte('!')
			if err := encodeValue(sb, fv, false); err != nil {
				return fmt.Errorf("field %q: %w", k, err)
			}
		}
		sb.WriteByte(')')
		return nil
	case []interface{}:
		if withTag {
			sb.WriteByte('a')
		}
		for _, elem := range val {
			if err := encodeValue(sb, elem, true); err != nil {
				return err
			}
		}
		sb.WriteByte('.')
		return nil
	default:
		return encodeNumber(sb, v, withTag)
	}
}

func encodeNumber(sb *strings.Builder, v interface{}, withTag bool) error {
	switch n := v.(type) {
	case int:
		return encodeInt(sb, int64(n), withTag)
	case int32:
		return encodeInt(sb, int64(n), withTag)
	case int64:
		return encodeInt(sb, n, withTag)
	case float64:
		if withTag {
			sb.WriteByte('j')
		}
		sb.WriteString(strconv.FormatFloat(n, 'g', -1, 64))
		sb.WriteByte('!')
		return nil
	case float32:
		return encodeNumber(sb, float64(n), withTag)
	default:
		return fmt.Errorf("mincode: cannot encode value of type %T", v)
	}
}

func encodeInt(sb *strings.Builder, n int64, withTag bool) error {
	if withTag {
		sb.WriteByte('d')
	}
	sb.WriteString(new(big.Int).SetInt64(n).Text(36))
	sb.WriteByte('!')
	return nil
}

// tagFor returns the type tag that would be used for v as a field's value
// (i.e. the character that also stands in for that field's name prefix).
func tagFor(v interface{}) (byte, error) {
	switch val := v.(type) {
	case nil:
		return 'n', nil
	case bool:
		if val {
			return 't', nil
		}
		return 'f', nil
	case string:
		return 's', nil
	case map[string]interface{}:
		// Always use '(' ... ')' rather than the 'o' empty-object
		// shorthand - readObject() handles an immediately-following ')'
		// (i.e. zero fields) the same way regardless, and this avoids the
		// tag-doubling trick's zero-payload special case entirely (a real
		// divolte.js client never emits 'o' either - see mincode.Decode's
		// docs).
		_ = val
		return '(', nil
	case []interface{}:
		return 'a', nil
	case int, int32, int64:
		return 'd', nil
	case float32, float64:
		return 'j', nil
	default:
		return 0, fmt.Errorf("mincode: cannot encode value of type %T", v)
	}
}

func writeEscaped(sb *strings.Builder, s string) {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '~' || c == '!' {
			sb.WriteByte('~')
		}
		sb.WriteByte(c)
	}
}
