package mincode

import (
	_ "embed"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

//go:embed testdata/mincode-samples.json
var samplesRaw string

type sample struct {
	Title string          `json:"title"`
	JSON  json.RawMessage `json:"json"`
	Code  string          `json:"code"`
}

func loadSamples(t *testing.T) []sample {
	t.Helper()
	// The fixture file (copied verbatim from divolte-collector's Java test
	// resources) has leading "//" comment lines, which encoding/json doesn't
	// accept - strip them before parsing.
	var lines []string
	for _, line := range strings.Split(samplesRaw, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "//") {
			continue
		}
		lines = append(lines, line)
	}
	var samples []sample
	if err := json.Unmarshal([]byte(strings.Join(lines, "\n")), &samples); err != nil {
		t.Fatalf("parsing mincode-samples.json: %v", err)
	}
	return samples
}

func TestSamplesFromJavaFixtures(t *testing.T) {
	for _, s := range loadSamples(t) {
		s := s
		t.Run(s.Title, func(t *testing.T) {
			var expected interface{}
			if err := json.Unmarshal(s.JSON, &expected); err != nil {
				t.Fatalf("parsing expected JSON: %v", err)
			}
			actual, err := Decode(s.Code)
			if err != nil {
				t.Fatalf("Decode(%q) error: %v", s.Code, err)
			}
			if !looseEqual(expected, actual) {
				t.Errorf("Decode(%q) = %#v, want %#v (from JSON %s)", s.Code, actual, expected, s.JSON)
			}
		})
	}
}

// looseEqual compares a value decoded from JSON (numbers as float64) against
// a value decoded from mincode (integers as int64, floats as float64),
// treating numerically-equal int64/float64 pairs as equal.
func looseEqual(expected, actual interface{}) bool {
	switch e := expected.(type) {
	case nil:
		return actual == nil
	case float64:
		switch a := actual.(type) {
		case int64:
			return e == float64(a)
		case float64:
			return e == a
		default:
			return false
		}
	case string, bool:
		return reflect.DeepEqual(e, actual)
	case map[string]interface{}:
		a, ok := actual.(map[string]interface{})
		if !ok || len(a) != len(e) {
			return false
		}
		for k, ev := range e {
			av, ok := a[k]
			if !ok || !looseEqual(ev, av) {
				return false
			}
		}
		return true
	case []interface{}:
		a, ok := actual.([]interface{})
		if !ok || len(a) != len(e) {
			return false
		}
		for i := range e {
			if !looseEqual(e[i], a[i]) {
				return false
			}
		}
		return true
	default:
		return reflect.DeepEqual(expected, actual)
	}
}

func TestUnicodeStringPassesThroughRaw(t *testing.T) {
	// Confirms no \uXXXX-style escaping: raw multi-byte UTF-8 sequences pass
	// straight through, only '~' and '!' are ever escaped.
	got, err := Decode("sH̶ello~!!")
	if err != nil {
		t.Fatalf("Decode error: %v", err)
	}
	want := "H̶ello!"
	if got != want {
		t.Errorf("Decode = %q, want %q", got, want)
	}
}

func TestFieldNameTagDoublesAsValueTag(t *testing.T) {
	// "dage!16!" as a field: leading 'd' is NOT part of the field name -
	// it's the type tag for the value that follows the name, so this
	// decodes to {"age": 42} not {"dage": ...}.
	got, err := Decode("(dage!16!)")
	if err != nil {
		t.Fatalf("Decode error: %v", err)
	}
	m, ok := got.(map[string]interface{})
	if !ok {
		t.Fatalf("got %#v, want map", got)
	}
	if v, ok := m["age"]; !ok || v != int64(42) {
		t.Errorf("m[\"age\"] = %#v, ok=%v, want int64(42)", v, ok)
	}
}

func TestEmptyObjectShorthand(t *testing.T) {
	// The field's leading tag char ('o') is itself the value-type tag for
	// the empty-object shorthand - it is not part of the field name and
	// has no payload, so "(oa!)" decodes to {"a": {}}.
	got, err := Decode("(oa!)")
	if err != nil {
		t.Fatalf("Decode error: %v", err)
	}
	m := got.(map[string]interface{})
	if v, ok := m["a"].(map[string]interface{}); !ok || len(v) != 0 {
		t.Errorf("m[\"a\"] = %#v, want empty map", m["a"])
	}
}
