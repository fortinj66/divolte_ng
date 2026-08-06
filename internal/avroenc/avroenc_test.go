package avroenc

import (
	"fmt"
	"testing"
)

// A trimmed schema covering representative type shapes seen in a typical
// production schema: required string/boolean/long/int (no default),
// nullable-int-with-default, nullable-array-of-string-with-null-default.
const testSchema = `{
  "namespace": "test.record",
  "type": "record",
  "name": "trimmed",
  "fields": [
    { "name": "detectedDuplicate", "type": "boolean" },
    { "name": "timestamp", "type": "long" },
    { "name": "partyId", "type": "string" },
    { "name": "quantity", "type": ["int", "null"], "default": 0 },
    { "name": "referer", "type": ["null", "string"], "default": null },
    { "name": "sku_id", "type": ["null", {"type": "array", "items": "string"}], "default": null }
  ]
}`

func TestEncodeDecodeRoundTrip(t *testing.T) {
	codec, err := LoadSchema(testSchema)
	if err != nil {
		t.Fatalf("LoadSchema: %v", err)
	}

	fields := map[string]interface{}{
		"detectedDuplicate": true,
		"timestamp":         int64(1690000000123),
		"partyId":           "0:1:party",
		"quantity":          int32(7),
		"referer":           "https://example.com/",
		"sku_id":            []interface{}{"sku-a", "sku-b"},
	}

	encoded, err := codec.EncodeNaked(fields)
	if err != nil {
		t.Fatalf("EncodeNaked: %v", err)
	}
	if len(encoded) == 0 {
		t.Fatal("EncodeNaked produced empty output")
	}

	decoded, err := codec.DecodeNaked(encoded)
	if err != nil {
		t.Fatalf("DecodeNaked: %v", err)
	}
	if decoded["partyId"] != "0:1:party" {
		t.Errorf("partyId = %v, want 0:1:party", decoded["partyId"])
	}
	if decoded["timestamp"] != int64(1690000000123) {
		t.Errorf("timestamp = %v, want 1690000000123", decoded["timestamp"])
	}
	if decoded["detectedDuplicate"] != true {
		t.Errorf("detectedDuplicate = %v, want true", decoded["detectedDuplicate"])
	}
}

func TestNullableFieldsOmittedFromMapUseSchemaDefault(t *testing.T) {
	codec, err := LoadSchema(testSchema)
	if err != nil {
		t.Fatalf("LoadSchema: %v", err)
	}

	// referer and sku_id are omitted entirely (as internal/mapping does for
	// an absent producer with no configured Default) - the nullable-with-
	// null-default schema fields should encode fine, resolving to null.
	fields := map[string]interface{}{
		"detectedDuplicate": false,
		"timestamp":         int64(1),
		"partyId":           "0:1:party",
	}

	encoded, err := codec.EncodeNaked(fields)
	if err != nil {
		t.Fatalf("EncodeNaked with omitted nullable fields: %v", err)
	}
	decoded, err := codec.DecodeNaked(encoded)
	if err != nil {
		t.Fatalf("DecodeNaked: %v", err)
	}
	if decoded["referer"] != nil {
		t.Errorf("referer = %v, want nil (schema default)", decoded["referer"])
	}
	if decoded["sku_id"] != nil {
		t.Errorf("sku_id = %v, want nil (schema default)", decoded["sku_id"])
	}
	if fmt.Sprint(decoded["quantity"]) != "0" {
		t.Errorf("quantity = %v (%T), want 0 (schema default)", decoded["quantity"], decoded["quantity"])
	}
}

func TestEncodeArrayFieldCoercesShapeMismatchesLikeJackson(t *testing.T) {
	codec, err := LoadSchema(testSchema)
	if err != nil {
		t.Fatalf("LoadSchema: %v", err)
	}
	// Real clients occasionally send a bare scalar (store_id=123) or an
	// array with wrong-typed elements for a field the schema types as
	// array<string>. Confirmed against a real legacy instance via
	// cmd/paritycheck: legacy's JSON->Avro converter has Jackson's
	// ACCEPT_SINGLE_VALUE_AS_ARRAY enabled, so a scalar becomes a
	// single-element array (and a wrong-typed element gets Jackson's
	// asText()-style stringification) instead of failing the whole record.
	for name, tc := range map[string]struct {
		value interface{}
		want  []string
	}{
		"bare scalar":            {int64(123), []string{"123"}},
		"array of wrong element": {[]interface{}{int64(1), int64(2)}, []string{"1", "2"}},
	} {
		t.Run(name, func(t *testing.T) {
			fields := map[string]interface{}{
				"detectedDuplicate": true,
				"timestamp":         int64(1),
				"partyId":           "0:1:party",
				"sku_id":            tc.value,
			}
			encoded, err := codec.EncodeNaked(fields)
			if err != nil {
				t.Fatalf("EncodeNaked: %v", err)
			}
			decoded, err := codec.DecodeNaked(encoded)
			if err != nil {
				t.Fatalf("DecodeNaked: %v", err)
			}
			got := asStringSlice(t, decoded["sku_id"])
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Errorf("sku_id = %v, want %v", got, tc.want)
			}
			if decoded["partyId"] != "0:1:party" {
				t.Errorf("partyId = %v, want 0:1:party (rest of record unaffected)", decoded["partyId"])
			}
		})
	}
}

func TestEncodeArrayFieldFallsBackToDefaultOnGenuinelyUnconvertibleElement(t *testing.T) {
	codec, err := LoadSchema(testSchema)
	if err != nil {
		t.Fatalf("LoadSchema: %v", err)
	}
	// A nested map/slice element has no Jackson asText()-style
	// stringification - this is the one shape that still can't be
	// represented, so it falls back to the field's schema default rather
	// than failing the whole record.
	fields := map[string]interface{}{
		"detectedDuplicate": true,
		"timestamp":         int64(1),
		"partyId":           "0:1:party",
		"sku_id":            []interface{}{map[string]interface{}{"nested": true}},
	}
	encoded, err := codec.EncodeNaked(fields)
	if err != nil {
		t.Fatalf("EncodeNaked: %v", err)
	}
	decoded, err := codec.DecodeNaked(encoded)
	if err != nil {
		t.Fatalf("DecodeNaked: %v", err)
	}
	if decoded["sku_id"] != nil {
		t.Errorf("sku_id = %v, want nil (schema default)", decoded["sku_id"])
	}
}

func TestEncodeFailsWhenRequiredFieldMissing(t *testing.T) {
	codec, err := LoadSchema(testSchema)
	if err != nil {
		t.Fatalf("LoadSchema: %v", err)
	}
	// "partyId" (required string, no default) is omitted - this must error,
	// not silently encode garbage or panic. Callers (the HTTP/Kafka
	// pipeline) are expected to catch this, log, and drop just that one
	// event rather than crash the server.
	fields := map[string]interface{}{
		"detectedDuplicate": true,
		"timestamp":         int64(1),
	}
	if _, err := codec.EncodeNaked(fields); err == nil {
		t.Error("expected an error when a required field with no default is missing")
	}
}

// asStringSlice accepts either []string or []interface{} of strings -
// hamba/avro's generic decoder may produce either depending on version/path,
// and callers of DecodeNaked shouldn't need to care which.
func asStringSlice(t *testing.T, v interface{}) []string {
	t.Helper()
	switch s := v.(type) {
	case []string:
		return s
	case []interface{}:
		out := make([]string, len(s))
		for i, e := range s {
			str, ok := e.(string)
			if !ok {
				t.Fatalf("element %d is %T, want string", i, e)
			}
			out[i] = str
		}
		return out
	default:
		t.Fatalf("value is %T, want []string or []interface{}", v)
		return nil
	}
}
