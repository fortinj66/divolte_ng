// Package avroenc loads a .avsc schema and encodes the generic
// map[string]interface{} produced by internal/mapping into naked Avro
// binary (no Confluent wire-format header), matching the production Kafka
// sink's `mode = naked` configuration.
package avroenc

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/hamba/avro/v2"
)

// Codec wraps a parsed Avro schema for repeated encoding.
type Codec struct {
	schema    avro.Schema
	record    *avro.RecordSchema
	arrayType map[string]arrayFieldInfo // field name -> array item schema, for fields whose (possibly-nullable) type is an array
}

type arrayFieldInfo struct {
	items   avro.Schema
	inUnion bool // true if the array is one branch of a union (e.g. nullable array) - hamba/avro's generic encoder requires complex-typed union branches to be wrapped as map[string]interface{}{"array": value}, unlike primitive branches (e.g. a nullable string) which accept a bare value.
}

// LoadSchemaFile parses a .avsc file (e.g. configs/example/schema.avsc).
// A legacy-exported schema file may contain "//" line comments - tolerated
// by the Java Avro tooling that originally loaded it, but not valid JSON -
// so these are stripped before parsing.
func LoadSchemaFile(path string) (*Codec, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("avroenc: reading %s: %w", path, err)
	}
	return LoadSchema(StripLineComments(string(data)))
}

// LoadSchema parses schema JSON text directly (no comment stripping - use
// LoadSchemaFile for files that may contain "//" comments).
func LoadSchema(schemaJSON string) (*Codec, error) {
	s, err := avro.Parse(schemaJSON)
	if err != nil {
		return nil, fmt.Errorf("avroenc: parsing schema: %w", err)
	}
	c := &Codec{schema: s, arrayType: map[string]arrayFieldInfo{}}
	if rs, ok := s.(*avro.RecordSchema); ok {
		c.record = rs
		for _, f := range rs.Fields() {
			if info, ok := arrayFieldInfoOf(f.Type()); ok {
				c.arrayType[f.Name()] = info
			}
		}
	}
	return c, nil
}

// arrayFieldInfoOf reports whether t is an array schema, or a union
// containing one (i.e. a nullable array field).
func arrayFieldInfoOf(t avro.Schema) (arrayFieldInfo, bool) {
	switch s := t.(type) {
	case *avro.ArraySchema:
		return arrayFieldInfo{items: s.Items(), inUnion: false}, true
	case *avro.UnionSchema:
		for _, member := range s.Types() {
			if arr, ok := member.(*avro.ArraySchema); ok {
				return arrayFieldInfo{items: arr.Items(), inUnion: true}, true
			}
		}
	}
	return arrayFieldInfo{}, false
}

// StripLineComments removes "//...end of line" comments that appear
// outside of JSON string literals. This is deliberately simple (no block
// comments) since that is all the real schema file uses.
func StripLineComments(s string) string {
	var out strings.Builder
	inString := false
	escaped := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if inString {
			out.WriteByte(c)
			if escaped {
				escaped = false
			} else if c == '\\' {
				escaped = true
			} else if c == '"' {
				inString = false
			}
			continue
		}
		if c == '"' {
			inString = true
			out.WriteByte(c)
			continue
		}
		if c == '/' && i+1 < len(s) && s[i+1] == '/' {
			for i < len(s) && s[i] != '\n' {
				i++
			}
			out.WriteByte('\n')
			continue
		}
		out.WriteByte(c)
	}
	return out.String()
}

// HasField reports whether the schema this Codec was loaded from declares a
// field with the given name - used at startup to validate that every
// mapping rule targets a real schema field, matching legacy's fail-fast
// config validation (ValidatedConfiguration + the constraint/*.java
// classes, e.g. MappingSourceSinkReferencesMustExist) rather than silently
// producing a schema this mapping doesn't actually match.
func (c *Codec) HasField(name string) bool {
	if c.record == nil {
		return false
	}
	for _, f := range c.record.Fields() {
		if f.Name() == name {
			return true
		}
	}
	return false
}

// EncodeNaked encodes fields (as produced by mapping.Config.Evaluate) into
// raw Avro binary - no Confluent schema-registry header, matching the
// production `kafka-web-combined` sink's `mode = naked`. Array-typed field
// values are expected as []interface{} (as internal/mincode/internal/
// mapping produce them) and are converted to the concrete slice type the
// schema requires (e.g. []string) - hamba/avro's generic encoder does not
// accept []interface{} directly.
//
// A real client occasionally sends a bare scalar for one of these fields -
// e.g. store_id=123 instead of store_id=[123]. The original Java mapping
// engine's JSON->Avro converter has Jackson's
// DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY enabled
// (AvroGenericRecordMapper.java), which wraps a lone scalar into a
// single-element array rather than failing - confirmed against a real
// legacy instance via cmd/paritycheck (a scalar cart_product_id/my_list_sku/
// product_type all came through on the real topic as one-element arrays).
// We match that instead of dropping the field to its schema default.
//
// EncodeNaked takes ownership of fields and may mutate it in place - the
// only caller (httpserver.processEvent) builds fields fresh per event from
// mapping.Config.Evaluate and never reads it again afterward, so a
// defensive copy here would only add per-event map-allocation overhead
// (multiplied by the real ~210-field schema) with no actual benefit.
func (c *Codec) EncodeNaked(fields map[string]interface{}) ([]byte, error) {
	for name, info := range c.arrayType {
		v, ok := fields[name]
		if !ok {
			continue
		}
		arr, isArray := v.([]interface{})
		if !isArray {
			arr = []interface{}{v}
		}
		converted, err := convertArray(arr, info.items)
		if err != nil {
			log.Printf("avroenc: field %q: %v - using schema default", name, err)
			delete(fields, name)
			continue
		}
		if info.inUnion {
			fields[name] = map[string]interface{}{"array": converted}
		} else {
			fields[name] = converted
		}
	}

	b, err := avro.Marshal(c.schema, fields)
	if err != nil {
		return nil, fmt.Errorf("avroenc: encoding record: %w", err)
	}
	return b, nil
}

func convertArray(arr []interface{}, itemSchema avro.Schema) (interface{}, error) {
	ps, ok := itemSchema.(*avro.PrimitiveSchema)
	if !ok {
		return nil, fmt.Errorf("unsupported array item schema %T", itemSchema)
	}
	switch ps.Type() {
	case avro.String:
		// A wrong-typed element (e.g. a number where a string array is
		// expected) gets the same Jackson-backed stringification legacy's
		// JSON->Avro converter applies elsewhere (see resolve()'s
		// stringifyLikeJackson in internal/mapping) rather than failing -
		// only a genuinely unconvertible element (a nested object/map)
		// still errors out.
		out := make([]string, len(arr))
		for i, v := range arr {
			s, ok := stringifyArrayElement(v)
			if !ok {
				return nil, fmt.Errorf("array element %d is %T, want string", i, v)
			}
			out[i] = s
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unsupported array item type %v (only string arrays are used by the production schema)", ps.Type())
	}
}

// stringifyArrayElement mirrors Jackson's JsonNode.asText() coercion that
// legacy's JSON->Avro converter applies to array elements - a number or
// boolean stringifies rather than failing; only a genuinely unconvertible
// value (a nested map/slice) reports !ok.
func stringifyArrayElement(v interface{}) (string, bool) {
	switch n := v.(type) {
	case string:
		return n, true
	case bool:
		return strconv.FormatBool(n), true
	case int64:
		return strconv.FormatInt(n, 10), true
	case float64:
		return strconv.FormatFloat(n, 'g', -1, 64), true
	default:
		return "", false
	}
}

// DecodeNaked is the inverse of EncodeNaked - mainly useful for tests and
// the parity-verification harness (decoding both the Go and Java servers'
// Kafka output for comparison). The result mirrors EncodeNaked's expected
// input shape: array-in-union fields are unwrapped back to a bare slice
// rather than hamba/avro's internal map[string]interface{}{"array": ...}
// generic union representation.
func (c *Codec) DecodeNaked(data []byte) (map[string]interface{}, error) {
	var out map[string]interface{}
	if err := avro.Unmarshal(c.schema, data, &out); err != nil {
		return nil, fmt.Errorf("avroenc: decoding record: %w", err)
	}
	for name, info := range c.arrayType {
		if !info.inUnion {
			continue
		}
		wrapped, ok := out[name].(map[string]interface{})
		if !ok {
			continue // was null, or already unwrapped
		}
		if arr, ok := wrapped["array"]; ok {
			out[name] = arr
		}
	}
	return out, nil
}
