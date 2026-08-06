package jsonenc

import (
	"encoding/json"
	"testing"
)

func TestEncode(t *testing.T) {
	fields := map[string]interface{}{
		"partyId": "abc123",
		"count":   5,
	}
	b, err := Encode(fields)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	var got map[string]interface{}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("unmarshaling result: %v", err)
	}
	if got["partyId"] != "abc123" {
		t.Errorf("partyId = %v, want abc123", got["partyId"])
	}

	// fields must be unmodified after Encode (unlike avroenc.EncodeNaked).
	if len(fields) != 2 {
		t.Errorf("fields mutated: %v", fields)
	}
}
