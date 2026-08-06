package druid

import "testing"

func TestDruidTypeFor(t *testing.T) {
	cases := []struct {
		in        string
		wantType  string
		wantArray bool
	}{
		{`"string"`, "string", false},
		{`["null","string"]`, "string", false},
		{`["null","long"]`, "long", false},
		{`["null","int"]`, "long", false},
		{`["null","double"]`, "double", false},
		{`["null","float"]`, "double", false},
		{`["null",{"type":"array","items":"string"}]`, "string", true},
		{`["null",{"type":"array","items":"long"}]`, "long", true},
		{`"boolean"`, "string", false},
	}
	for _, c := range cases {
		gotType, gotArray := druidTypeFor(c.in)
		if gotType != c.wantType || gotArray != c.wantArray {
			t.Errorf("druidTypeFor(%q) = (%q, %v), want (%q, %v)", c.in, gotType, gotArray, c.wantType, c.wantArray)
		}
	}
}

func TestAccountedForNamesIncludesDimensionsAndExclusions(t *testing.T) {
	dimensionsSpec := map[string]interface{}{
		"dimensions": []interface{}{
			map[string]interface{}{"name": "sku_id", "type": "string"},
			"store_id", // bare-string shorthand form
		},
		"dimensionExclusions": []interface{}{"__time", "timestamp"},
	}
	names := accountedForNames(dimensionsSpec)
	for _, want := range []string{"sku_id", "store_id", "__time", "timestamp"} {
		if !names[want] {
			t.Errorf("accountedForNames missing %q, got %v", want, names)
		}
	}
	if names["not_present"] {
		t.Error("accountedForNames should not report an unrelated name as present")
	}
}

// TestSyncFieldsSkipsExcludedTimestampField is a regression test for a
// real bug found via a live end-to-end test against the actual dev
// cluster: the Go schema has a "timestamp" field (used for Druid's
// timestampSpec), which Druid's dimensionsSpec deliberately EXCLUDES via
// dimensionExclusions rather than listing it as a normal dimension. Before
// accountedForNames existed, SyncFields treated "timestamp" as a missing
// dimension and tried to add it, which Druid rejected outright
// ("dimensions and dimensions exclusions cannot overlap").
func TestSyncFieldsSkipsExcludedTimestampField(t *testing.T) {
	dimensionsSpec := map[string]interface{}{
		"dimensions":          []interface{}{},
		"dimensionExclusions": []interface{}{"__time", "timestamp"},
	}
	names := accountedForNames(dimensionsSpec)
	if !names["timestamp"] {
		t.Fatal("accountedForNames must treat an excluded field as already accounted for")
	}
}

func TestNewRequiresBaseURLAndSupervisorName(t *testing.T) {
	if _, err := New(Config{SupervisorName: "x"}); err == nil {
		t.Error("New with no BaseURL should error")
	}
	if _, err := New(Config{BaseURL: "http://example.invalid"}); err == nil {
		t.Error("New with no SupervisorName should error")
	}
	if _, err := New(Config{BaseURL: "http://example.invalid", SupervisorName: "x"}); err != nil {
		t.Errorf("New with valid config should succeed, got: %v", err)
	}
}
