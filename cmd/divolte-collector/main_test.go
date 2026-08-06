package main

import (
	"testing"

	"github.com/example/divolte-rewrite/internal/avroenc"
	"github.com/example/divolte-rewrite/internal/mapping"
)

const testSchema = `{
  "namespace": "test.record", "type": "record", "name": "trimmed",
  "fields": [
    { "name": "partyId", "type": "string" },
    { "name": "quantity", "type": ["int", "null"], "default": 0 }
  ]
}`

func TestUnknownMappingFieldsCatchesFieldNotInSchema(t *testing.T) {
	codec, err := avroenc.LoadSchema(testSchema)
	if err != nil {
		t.Fatalf("LoadSchema: %v", err)
	}
	mappingCfg := &mapping.Config{Fields: []mapping.FieldRule{
		{Field: "partyId", Builtin: "partyId"},
		{Field: "quantity", EventParam: "quantity", Coerce: "int32"},
		{Field: "this_field_does_not_exist_in_the_schema", EventParam: "whatever"},
	}}

	missing := unknownMappingFields(mappingCfg, codec)
	if len(missing) != 1 || missing[0] != "this_field_does_not_exist_in_the_schema" {
		t.Errorf("unknownMappingFields = %v, want exactly [this_field_does_not_exist_in_the_schema]", missing)
	}
}

func TestUnknownMappingFieldsCleanWhenAllFieldsExist(t *testing.T) {
	codec, err := avroenc.LoadSchema(testSchema)
	if err != nil {
		t.Fatalf("LoadSchema: %v", err)
	}
	mappingCfg := &mapping.Config{Fields: []mapping.FieldRule{
		{Field: "partyId", Builtin: "partyId"},
		{Field: "quantity", EventParam: "quantity", Coerce: "int32"},
	}}

	if missing := unknownMappingFields(mappingCfg, codec); len(missing) != 0 {
		t.Errorf("unknownMappingFields = %v, want none", missing)
	}
}

func TestSnapshotSignatureStableWhenNothingChanges(t *testing.T) {
	mappingCfg := &mapping.Config{Fields: []mapping.FieldRule{{Field: "partyId", Builtin: "partyId"}}}

	sig1, err := snapshotSignature(testSchema, mappingCfg)
	if err != nil {
		t.Fatalf("snapshotSignature: %v", err)
	}
	sig2, err := snapshotSignature(testSchema, mappingCfg)
	if err != nil {
		t.Fatalf("snapshotSignature: %v", err)
	}
	if sig1 != sig2 {
		t.Errorf("signature changed with identical inputs: %q vs %q", sig1, sig2)
	}
}

func TestSnapshotSignatureChangesWithSchema(t *testing.T) {
	mappingCfg := &mapping.Config{Fields: []mapping.FieldRule{{Field: "partyId", Builtin: "partyId"}}}
	otherSchema := `{"namespace":"test.record","type":"record","name":"trimmed","fields":[{"name":"partyId","type":"string"}]}`

	sig1, err := snapshotSignature(testSchema, mappingCfg)
	if err != nil {
		t.Fatalf("snapshotSignature: %v", err)
	}
	sig2, err := snapshotSignature(otherSchema, mappingCfg)
	if err != nil {
		t.Fatalf("snapshotSignature: %v", err)
	}
	if sig1 == sig2 {
		t.Error("signature unchanged despite a different schema")
	}
}

func TestSnapshotSignatureChangesWithMappingOnlyEdit(t *testing.T) {
	// Same schema JSON both times - only the mapping rule differs (which
	// event_param feeds "partyId") - the signature must still change, since
	// comparing schema JSON alone would miss exactly this kind of edit.
	mappingCfg1 := &mapping.Config{Fields: []mapping.FieldRule{{Field: "partyId", Builtin: "partyId"}}}
	mappingCfg2 := &mapping.Config{Fields: []mapping.FieldRule{{Field: "partyId", EventParam: "p"}}}

	sig1, err := snapshotSignature(testSchema, mappingCfg1)
	if err != nil {
		t.Fatalf("snapshotSignature: %v", err)
	}
	sig2, err := snapshotSignature(testSchema, mappingCfg2)
	if err != nil {
		t.Fatalf("snapshotSignature: %v", err)
	}
	if sig1 == sig2 {
		t.Error("signature unchanged despite a mapping-only edit")
	}
}
