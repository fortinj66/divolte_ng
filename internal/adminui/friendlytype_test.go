package adminui

import "testing"

func TestParseFriendlyTypeCoversRealSchemaShapes(t *testing.T) {
	cases := []struct {
		name        string
		typeJSON    string
		hasDefault  bool
		defaultJSON string
		want        friendlyType
	}{
		{
			name:     "bare required string (partyId)",
			typeJSON: `"string"`,
			want:     friendlyType{BaseType: "string", ItemType: "string", DefaultMode: "none"},
		},
		{
			name:     "bare required boolean (detectedDuplicate)",
			typeJSON: `"boolean"`,
			want:     friendlyType{BaseType: "boolean", ItemType: "string", DefaultMode: "none"},
		},
		{
			name:        "nullable string, null default (referer)",
			typeJSON:    `["null","string"]`,
			hasDefault:  true,
			defaultJSON: `null`,
			want:        friendlyType{BaseType: "string", ItemType: "string", IsNullable: true, DefaultMode: "null"},
		},
		{
			name:        "nullable int, literal default (quantity)",
			typeJSON:    `["int","null"]`,
			hasDefault:  true,
			defaultJSON: `0`,
			want:        friendlyType{BaseType: "int", ItemType: "string", IsNullable: true, DefaultMode: "value", DefaultText: "0"},
		},
		{
			name:        "nullable array of string, null default (sku_id)",
			typeJSON:    `["null",{"type":"array","items":"string"}]`,
			hasDefault:  true,
			defaultJSON: `null`,
			want:        friendlyType{BaseType: "string", ItemType: "string", IsArray: true, IsNullable: true, DefaultMode: "null"},
		},
		{
			name:        "nullable double, negative literal default",
			typeJSON:    `["double","null"]`,
			hasDefault:  true,
			defaultJSON: `-1`,
			want:        friendlyType{BaseType: "double", ItemType: "string", IsNullable: true, DefaultMode: "value", DefaultText: "-1"},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := parseFriendlyType(c.typeJSON, c.hasDefault, c.defaultJSON)
			if got.UseAdvanced {
				t.Fatalf("expected a friendly (non-advanced) parse, got advanced fallback for %+v", got)
			}
			if got.BaseType != c.want.BaseType || got.ItemType != c.want.ItemType || got.IsArray != c.want.IsArray ||
				got.IsNullable != c.want.IsNullable || got.DefaultMode != c.want.DefaultMode || got.DefaultText != c.want.DefaultText {
				t.Errorf("got %+v, want %+v", got, c.want)
			}
		})
	}
}

func TestParseFriendlyTypeFallsBackToAdvancedForExoticShapes(t *testing.T) {
	cases := []string{
		`{"type":"record","name":"x","fields":[]}`,
		`["null","string","int"]`, // 3-branch union
		`["null",{"type":"map","values":"string"}]`,
		`{"type":"enum","name":"x","symbols":["A"]}`,
	}
	for _, typeJSON := range cases {
		got := parseFriendlyType(typeJSON, false, "")
		if !got.UseAdvanced {
			t.Errorf("Parse(%s): expected advanced fallback, got friendly %+v", typeJSON, got)
		}
		if got.AdvancedType != typeJSON {
			t.Errorf("AdvancedType = %q, want %q preserved verbatim", got.AdvancedType, typeJSON)
		}
	}
}

func TestBuildTypeAndDefaultRoundTripsWithParse(t *testing.T) {
	cases := []struct {
		name string
		ft   friendlyType
	}{
		{"required string", friendlyType{BaseType: "string", DefaultMode: "none"}},
		{"required boolean", friendlyType{BaseType: "boolean", DefaultMode: "none"}},
		{"nullable string null default", friendlyType{BaseType: "string", IsNullable: true, DefaultMode: "null"}},
		{"nullable int literal default", friendlyType{BaseType: "int", IsNullable: true, DefaultMode: "value", DefaultText: "0"}},
		{"nullable array of string null default", friendlyType{BaseType: "string", ItemType: "string", IsArray: true, IsNullable: true, DefaultMode: "null"}},
		{"nullable double negative default", friendlyType{BaseType: "double", IsNullable: true, DefaultMode: "value", DefaultText: "-1.5"}},
		{"nullable boolean true default", friendlyType{BaseType: "boolean", IsNullable: true, DefaultMode: "value", DefaultText: "true"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			typeJSON, hasDefault, defaultJSON, err := buildTypeAndDefault(c.ft)
			if err != nil {
				t.Fatalf("buildTypeAndDefault: %v", err)
			}
			reparsed := parseFriendlyType(typeJSON, hasDefault, defaultJSON)
			if reparsed.UseAdvanced {
				t.Fatalf("round trip fell back to advanced: type=%s default=%s", typeJSON, defaultJSON)
			}
			if reparsed.BaseType != c.ft.BaseType || reparsed.IsArray != c.ft.IsArray ||
				reparsed.IsNullable != c.ft.IsNullable || reparsed.DefaultMode != c.ft.DefaultMode ||
				reparsed.DefaultText != c.ft.DefaultText {
				t.Errorf("round trip mismatch: got %+v, want %+v (via type=%s default=%s)", reparsed, c.ft, typeJSON, defaultJSON)
			}
		})
	}
}

func TestBuildTypeAndDefaultValidatesScalarDefault(t *testing.T) {
	_, _, _, err := buildTypeAndDefault(friendlyType{BaseType: "int", IsNullable: true, DefaultMode: "value", DefaultText: "not-a-number"})
	if err == nil {
		t.Error("expected an error for a non-numeric int default")
	}
}

func TestBuildTypeAndDefaultRejectsArrayLiteralDefault(t *testing.T) {
	_, _, _, err := buildTypeAndDefault(friendlyType{BaseType: "string", IsArray: true, IsNullable: true, DefaultMode: "value", DefaultText: "x"})
	if err == nil {
		t.Error("expected an error for an array field with a literal (non-null) default in friendly mode")
	}
}

func TestBuildTypeAndDefaultUsesAdvancedOverride(t *testing.T) {
	typeJSON, hasDefault, defaultJSON, err := buildTypeAndDefault(friendlyType{
		UseAdvanced: true, AdvancedType: `{"type":"map","values":"string"}`,
	})
	if err != nil {
		t.Fatalf("buildTypeAndDefault: %v", err)
	}
	if typeJSON != `{"type":"map","values":"string"}` || hasDefault {
		t.Errorf("got type=%s hasDefault=%v", typeJSON, hasDefault)
	}
	_ = defaultJSON
}
