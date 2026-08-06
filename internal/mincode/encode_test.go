package mincode

import (
	"reflect"
	"testing"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	cases := []interface{}{
		"a string",
		"Hello~World!",
		true,
		false,
		nil,
		int64(42),
		int64(-37),
		3.14,
		map[string]interface{}{},
		[]interface{}{},
		map[string]interface{}{"foo": "bar", "baz": "daz"},
		[]interface{}{"foo", "bar"},
		map[string]interface{}{
			"a": map[string]interface{}{},
			"b": "c",
			"d": map[string]interface{}{"a": []interface{}{}, "b": "g"},
			"e": []interface{}{"1", "2"},
			"f": int64(42),
			"j": true,
			"k": false,
			"l": nil,
		},
	}

	for _, want := range cases {
		encoded, err := Encode(want)
		if err != nil {
			t.Fatalf("Encode(%#v): %v", want, err)
		}
		got, err := Decode(encoded)
		if err != nil {
			t.Fatalf("Decode(%q) (from encoding %#v): %v", encoded, want, err)
		}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("round trip mismatch:\n  original: %#v\n  encoded:  %q\n  decoded:  %#v", want, encoded, got)
		}
	}
}

func TestEncodeEmptyObjectNeverUsesOShorthand(t *testing.T) {
	encoded, err := Encode(map[string]interface{}{})
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if encoded != "()" {
		t.Errorf("Encode(empty map) = %q, want \"()\" (matching what a real divolte.js client emits)", encoded)
	}
}

func TestEncodeRealisticPayload(t *testing.T) {
	payload := map[string]interface{}{
		"environment_site": "shop.example.com",
		"quantity":         int64(5),
		"sku_id":           []interface{}{"sku-a", "sku-b"},
	}
	encoded, err := Encode(payload)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	decoded, err := Decode(encoded)
	if err != nil {
		t.Fatalf("Decode(%q): %v", encoded, err)
	}
	m := decoded.(map[string]interface{})
	if m["environment_site"] != "shop.example.com" {
		t.Errorf("environment_site = %v", m["environment_site"])
	}
	if m["quantity"] != int64(5) {
		t.Errorf("quantity = %v", m["quantity"])
	}
}
