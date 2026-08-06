package store

import (
	"database/sql"
	"fmt"
	"regexp"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/example/divolte-rewrite/internal/event"
	"github.com/example/divolte-rewrite/internal/mapping"
)

// Points at a throwaway local dev MariaDB container (see
// docs/admin-ui-guide.md) - not the real shared instance, which is
// reserved for after this development work completes.
const (
	testDBHost     = "localhost"
	testDBPort     = 3306
	testDBRootUser = "root"
	testDBRootPass = "devpass"
)

var testDBNameRe = regexp.MustCompile(`[^a-zA-Z0-9_]`)

// openTestStore gives each test its own throwaway database (created via a
// root connection, dropped on cleanup) so tests can run concurrently
// against the shared dev MariaDB without stepping on each other's rows -
// unlike the old one-SQLite-file-per-test approach, there's exactly one
// database server here, not one file per test.
func openTestStore(t *testing.T) *Store {
	t.Helper()

	dbName := "divolte_test_" + testDBNameRe.ReplaceAllString(t.Name(), "_")
	if len(dbName) > 64 {
		dbName = dbName[:64]
	}

	rootDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?multiStatements=true", testDBRootUser, testDBRootPass, testDBHost, testDBPort)
	admin, err := sql.Open("mysql", rootDSN)
	if err != nil {
		t.Fatalf("connecting to test MariaDB as root: %v", err)
	}

	if _, err := admin.Exec("DROP DATABASE IF EXISTS `" + dbName + "`"); err != nil {
		admin.Close()
		t.Fatalf("dropping stale test database %s: %v", dbName, err)
	}
	if _, err := admin.Exec("CREATE DATABASE `" + dbName + "`"); err != nil {
		admin.Close()
		t.Fatalf("creating test database %s: %v", dbName, err)
	}
	t.Cleanup(func() {
		if _, err := admin.Exec("DROP DATABASE IF EXISTS `" + dbName + "`"); err != nil {
			t.Logf("cleanup: dropping test database %s: %v", dbName, err)
		}
		admin.Close()
	})

	s, err := Open(Config{
		Host:     testDBHost,
		Port:     testDBPort,
		Name:     dbName,
		Username: testDBRootUser,
		Password: testDBRootPass,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestNewStoreIsEmpty(t *testing.T) {
	s := openTestStore(t)
	empty, err := s.IsEmpty()
	if err != nil {
		t.Fatalf("IsEmpty: %v", err)
	}
	if !empty {
		t.Error("expected a fresh store to be empty")
	}
}

func TestUpsertAndGetField(t *testing.T) {
	s := openTestStore(t)

	f := SchemaField{Name: "quantity", TypeJSON: `["int","null"]`, HasDefault: true, DefaultJSON: "0"}
	r := MappingRule{EventParam: "quantity", Coerce: "int32"}
	if err := s.UpsertField(f, r); err != nil {
		t.Fatalf("UpsertField: %v", err)
	}

	gotField, gotRule, err := s.GetField("quantity")
	if err != nil {
		t.Fatalf("GetField: %v", err)
	}
	if gotField == nil || gotField.TypeJSON != `["int","null"]` {
		t.Errorf("gotField = %+v", gotField)
	}
	if gotRule == nil || gotRule.EventParam != "quantity" || gotRule.Coerce != "int32" {
		t.Errorf("gotRule = %+v", gotRule)
	}

	// Upsert again with a change - should update, not duplicate.
	f.DefaultJSON = "1"
	if err := s.UpsertField(f, r); err != nil {
		t.Fatalf("UpsertField (update): %v", err)
	}
	fields, err := s.ListSchemaFields()
	if err != nil {
		t.Fatalf("ListSchemaFields: %v", err)
	}
	if len(fields) != 1 {
		t.Fatalf("expected 1 field after re-upsert, got %d", len(fields))
	}
	if fields[0].DefaultJSON != "1" {
		t.Errorf("DefaultJSON = %q, want 1 (updated)", fields[0].DefaultJSON)
	}
}

func TestDeleteFieldCascadesMappingRule(t *testing.T) {
	s := openTestStore(t)
	if err := s.UpsertField(SchemaField{Name: "x", TypeJSON: `"string"`}, MappingRule{EventParam: "x"}); err != nil {
		t.Fatalf("UpsertField: %v", err)
	}
	if err := s.DeleteField("x"); err != nil {
		t.Fatalf("DeleteField: %v", err)
	}
	f, r, err := s.GetField("x")
	if err != nil {
		t.Fatalf("GetField: %v", err)
	}
	if f != nil || r != nil {
		t.Errorf("expected field and rule to be gone, got field=%+v rule=%+v", f, r)
	}
}

// seedOrdered creates fields named a,b,c,d,e in that position order.
func seedOrdered(t *testing.T, s *Store) {
	t.Helper()
	for i, name := range []string{"a", "b", "c", "d", "e"} {
		if err := s.UpsertField(SchemaField{Name: name, TypeJSON: `"string"`, Position: i + 1}, MappingRule{EventParam: name}); err != nil {
			t.Fatalf("seeding %q: %v", name, err)
		}
	}
}

func orderedNames(t *testing.T, s *Store) []string {
	t.Helper()
	fields, err := s.ListSchemaFields()
	if err != nil {
		t.Fatalf("ListSchemaFields: %v", err)
	}
	names := make([]string, len(fields))
	for i, f := range fields {
		names[i] = f.Name
	}
	return names
}

func TestReorderBlockMoveUpSingleField(t *testing.T) {
	s := openTestStore(t)
	seedOrdered(t, s)
	if err := s.ReorderBlock([]string{"c"}, "up"); err != nil {
		t.Fatalf("ReorderBlock: %v", err)
	}
	got := orderedNames(t, s)
	want := []string{"a", "c", "b", "d", "e"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestReorderBlockMoveDownSingleField(t *testing.T) {
	s := openTestStore(t)
	seedOrdered(t, s)
	if err := s.ReorderBlock([]string{"b"}, "down"); err != nil {
		t.Fatalf("ReorderBlock: %v", err)
	}
	got := orderedNames(t, s)
	want := []string{"a", "c", "b", "d", "e"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestReorderBlockMoveMultipleUp(t *testing.T) {
	s := openTestStore(t)
	seedOrdered(t, s)
	if err := s.ReorderBlock([]string{"b", "c"}, "up"); err != nil {
		t.Fatalf("ReorderBlock: %v", err)
	}
	got := orderedNames(t, s)
	want := []string{"b", "c", "a", "d", "e"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestReorderBlockMoveMultipleDown(t *testing.T) {
	s := openTestStore(t)
	seedOrdered(t, s)
	if err := s.ReorderBlock([]string{"b", "c"}, "down"); err != nil {
		t.Fatalf("ReorderBlock: %v", err)
	}
	got := orderedNames(t, s)
	want := []string{"a", "d", "b", "c", "e"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestReorderBlockAtTopOrBottomIsNoOp(t *testing.T) {
	s := openTestStore(t)
	seedOrdered(t, s)
	if err := s.ReorderBlock([]string{"a"}, "up"); err != nil {
		t.Fatalf("ReorderBlock: %v", err)
	}
	if got := orderedNames(t, s); fmt.Sprint(got) != fmt.Sprint([]string{"a", "b", "c", "d", "e"}) {
		t.Errorf("expected no-op at top, got %v", got)
	}
	if err := s.ReorderBlock([]string{"e"}, "down"); err != nil {
		t.Fatalf("ReorderBlock: %v", err)
	}
	if got := orderedNames(t, s); fmt.Sprint(got) != fmt.Sprint([]string{"a", "b", "c", "d", "e"}) {
		t.Errorf("expected no-op at bottom, got %v", got)
	}
}

func TestReorderBlockRejectsNonConsecutiveSelection(t *testing.T) {
	s := openTestStore(t)
	seedOrdered(t, s)
	if err := s.ReorderBlock([]string{"a", "c"}, "up"); err == nil {
		t.Error("expected an error for a non-consecutive selection")
	}
	// The order must be unchanged after a rejected reorder.
	if got := orderedNames(t, s); fmt.Sprint(got) != fmt.Sprint([]string{"a", "b", "c", "d", "e"}) {
		t.Errorf("order should be unchanged after a rejected reorder, got %v", got)
	}
}

func TestReorderBlockRejectsUnknownField(t *testing.T) {
	s := openTestStore(t)
	seedOrdered(t, s)
	if err := s.ReorderBlock([]string{"nonexistent"}, "up"); err == nil {
		t.Error("expected an error for an unknown field")
	}
}

func TestReorderBlockRejectsEmptySelection(t *testing.T) {
	s := openTestStore(t)
	seedOrdered(t, s)
	if err := s.ReorderBlock(nil, "up"); err == nil {
		t.Error("expected an error for an empty selection")
	}
}

func TestSetFieldOrderAppliesExactOrder(t *testing.T) {
	s := openTestStore(t)
	seedOrdered(t, s)
	if err := s.SetFieldOrder([]string{"e", "a", "d", "b", "c"}); err != nil {
		t.Fatalf("SetFieldOrder: %v", err)
	}
	got := orderedNames(t, s)
	want := []string{"e", "a", "d", "b", "c"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSetFieldOrderRejectsWrongCount(t *testing.T) {
	s := openTestStore(t)
	seedOrdered(t, s)
	if err := s.SetFieldOrder([]string{"a", "b"}); err == nil {
		t.Error("expected an error when the new order omits fields")
	}
}

func TestSetFieldOrderRejectsUnknownOrDuplicateField(t *testing.T) {
	s := openTestStore(t)
	seedOrdered(t, s)
	if err := s.SetFieldOrder([]string{"a", "b", "c", "d", "nonexistent"}); err == nil {
		t.Error("expected an error for an unknown field")
	}
	if err := s.SetFieldOrder([]string{"a", "a", "c", "d", "e"}); err == nil {
		t.Error("expected an error for a duplicate field")
	}
}

func TestHasUnpublishedChangesBeforeAnySnapshot(t *testing.T) {
	s := openTestStore(t)
	if changes, err := s.HasUnpublishedChanges(); err != nil || changes {
		t.Errorf("empty store with no snapshot: changes=%v err=%v, want false,nil", changes, err)
	}
	seedOrdered(t, s)
	if changes, err := s.HasUnpublishedChanges(); err != nil || !changes {
		t.Errorf("non-empty store with no snapshot yet: changes=%v err=%v, want true,nil", changes, err)
	}
}

func TestSaveSnapshotClearsUnpublishedChanges(t *testing.T) {
	s := openTestStore(t)
	seedOrdered(t, s)
	if err := s.SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if changes, err := s.HasUnpublishedChanges(); err != nil || changes {
		t.Errorf("right after SaveSnapshot: changes=%v err=%v, want false,nil", changes, err)
	}

	// A reorder alone counts as a change, even with no field content edits.
	if err := s.ReorderBlock([]string{"b"}, "up"); err != nil {
		t.Fatalf("ReorderBlock: %v", err)
	}
	if changes, err := s.HasUnpublishedChanges(); err != nil || !changes {
		t.Errorf("after a reorder: changes=%v err=%v, want true,nil", changes, err)
	}
}

func TestRevertRestoresSnapshot(t *testing.T) {
	s := openTestStore(t)
	seedOrdered(t, s)
	if err := s.SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	// Make several kinds of changes: reorder, edit, add, delete.
	if err := s.ReorderBlock([]string{"b", "c"}, "up"); err != nil {
		t.Fatalf("ReorderBlock: %v", err)
	}
	if err := s.UpsertField(SchemaField{Name: "a", TypeJSON: `"int"`}, MappingRule{Builtin: "timestamp"}); err != nil {
		t.Fatalf("UpsertField (edit): %v", err)
	}
	if err := s.UpsertField(SchemaField{Name: "z", TypeJSON: `"string"`}, MappingRule{EventParam: "z"}); err != nil {
		t.Fatalf("UpsertField (add): %v", err)
	}
	if err := s.DeleteField("e"); err != nil {
		t.Fatalf("DeleteField: %v", err)
	}
	if changes, err := s.HasUnpublishedChanges(); err != nil || !changes {
		t.Fatalf("expected changes to be detected before revert: changes=%v err=%v", changes, err)
	}

	if err := s.Revert(); err != nil {
		t.Fatalf("Revert: %v", err)
	}

	got := orderedNames(t, s)
	want := []string{"a", "b", "c", "d", "e"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Errorf("order after revert: got %v, want %v", got, want)
	}
	f, r, err := s.GetField("a")
	if err != nil || f.TypeJSON != `"string"` || r.EventParam != "a" {
		t.Errorf("field 'a' not restored to original: field=%+v rule=%+v err=%v", f, r, err)
	}
	if changes, err := s.HasUnpublishedChanges(); err != nil || changes {
		t.Errorf("after revert: changes=%v err=%v, want false,nil", changes, err)
	}
}

func TestRevertErrorsWithoutAnySnapshot(t *testing.T) {
	s := openTestStore(t)
	seedOrdered(t, s)
	if err := s.Revert(); err == nil {
		t.Error("expected an error reverting when nothing has ever been published")
	}
}

func TestGetPublishedSnapshotReturnsPublishedNotEditBuffer(t *testing.T) {
	s := openTestStore(t)

	if _, _, err := s.GetPublishedSnapshot("test.ns", "rec"); err == nil {
		t.Fatal("expected an error before any snapshot exists")
	}

	seedOrdered(t, s)
	if err := s.SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}

	schemaJSON, mappingCfg, err := s.GetPublishedSnapshot("test.ns", "rec")
	if err != nil {
		t.Fatalf("GetPublishedSnapshot: %v", err)
	}
	if !strings.Contains(schemaJSON, `"name":"a"`) {
		t.Errorf("published schema JSON missing seeded field 'a': %s", schemaJSON)
	}
	wantRules := len(mappingCfg.Fields)
	if wantRules != 5 {
		t.Errorf("published mapping rule count = %d, want 5", wantRules)
	}

	// Edit further WITHOUT publishing again - GetPublishedSnapshot must
	// keep returning the earlier snapshot, not this in-progress edit.
	if err := s.UpsertField(SchemaField{Name: "z", TypeJSON: `"string"`}, MappingRule{EventParam: "z"}); err != nil {
		t.Fatalf("UpsertField: %v", err)
	}
	_, mappingCfg2, err := s.GetPublishedSnapshot("test.ns", "rec")
	if err != nil {
		t.Fatalf("GetPublishedSnapshot (after unpublished edit): %v", err)
	}
	if len(mappingCfg2.Fields) != wantRules {
		t.Errorf("GetPublishedSnapshot reflected an unpublished edit: rule count = %d, want unchanged %d", len(mappingCfg2.Fields), wantRules)
	}
}

func TestNewFieldAppendsAtEndByDefault(t *testing.T) {
	s := openTestStore(t)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("UpsertField: %v", err)
		}
	}
	must(s.UpsertField(SchemaField{Name: "a", TypeJSON: `"string"`, Position: 1}, MappingRule{EventParam: "a"}))
	must(s.UpsertField(SchemaField{Name: "b", TypeJSON: `"string"`, Position: 2}, MappingRule{EventParam: "b"}))
	must(s.UpsertField(SchemaField{Name: "c", TypeJSON: `"string"`}, MappingRule{EventParam: "c"})) // Position 0 => append

	fields, err := s.ListSchemaFields()
	if err != nil {
		t.Fatalf("ListSchemaFields: %v", err)
	}
	if len(fields) != 3 || fields[2].Name != "c" {
		t.Errorf("fields = %+v, want c last", fields)
	}
}

func TestBuildSchemaJSONProducesValidSchema(t *testing.T) {
	s := openTestStore(t)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("UpsertField: %v", err)
		}
	}
	must(s.UpsertField(SchemaField{Name: "partyId", TypeJSON: `"string"`}, MappingRule{Builtin: "partyId"}))
	must(s.UpsertField(SchemaField{Name: "referer", TypeJSON: `["null","string"]`, HasDefault: true, DefaultJSON: "null"}, MappingRule{Builtin: "referer"}))

	schemaJSON, err := s.BuildSchemaJSON("test.record", "trimmed")
	if err != nil {
		t.Fatalf("BuildSchemaJSON: %v", err)
	}
	if schemaJSON == "" {
		t.Fatal("empty schema JSON")
	}
}

func TestBuildMappingConfigRoundTripsWithEvaluate(t *testing.T) {
	s := openTestStore(t)
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("UpsertField: %v", err)
		}
	}
	must(s.UpsertField(SchemaField{Name: "partyId", TypeJSON: `"string"`}, MappingRule{Builtin: "partyId"}))
	must(s.UpsertField(SchemaField{Name: "quantity", TypeJSON: `["int","null"]`, HasDefault: true, DefaultJSON: "0"},
		MappingRule{EventParam: "quantity", Coerce: "int32"}))

	cfg, err := s.BuildMappingConfig()
	if err != nil {
		t.Fatalf("BuildMappingConfig: %v", err)
	}
	if len(cfg.Fields) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(cfg.Fields))
	}

	ev := testEventForStore()
	ctx := mapping.NewContext(ev, map[string]interface{}{"quantity": int64(7)}, false)
	out, err := cfg.Evaluate(ctx)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if out["quantity"] != int32(7) {
		t.Errorf("quantity = %v, want int32(7)", out["quantity"])
	}
	if out["partyId"] != ev.PartyID.String() {
		t.Errorf("partyId = %v, want %v", out["partyId"], ev.PartyID.String())
	}
}

func TestBuildMappingConfigSkipsSourcelessRules(t *testing.T) {
	// Regression test: UpsertField always writes a mapping_rules row
	// (paired with the field), even when no source is set yet (e.g. a
	// field added via the admin UI before its rule is configured, or the
	// real "cart_sku" quirk - see TestSeedFromFilesPreservesCartSkuQuirkAsSingleRule).
	// BuildMappingConfig must skip these, NOT hand them to
	// mapping.Config.Evaluate (which correctly errors on a genuinely
	// sourceless rule) - otherwise one such field breaks every event.
	s := openTestStore(t)
	if err := s.UpsertField(SchemaField{Name: "partyId", TypeJSON: `"string"`}, MappingRule{Builtin: "partyId"}); err != nil {
		t.Fatalf("UpsertField: %v", err)
	}
	if err := s.UpsertField(SchemaField{Name: "unmapped_field", TypeJSON: `["null","string"]`, HasDefault: true, DefaultJSON: "null"}, MappingRule{}); err != nil {
		t.Fatalf("UpsertField: %v", err)
	}

	cfg, err := s.BuildMappingConfig()
	if err != nil {
		t.Fatalf("BuildMappingConfig: %v", err)
	}
	for _, f := range cfg.Fields {
		if f.Field == "unmapped_field" {
			t.Fatalf("expected unmapped_field to be excluded from the mapping config, got rule %+v", f)
		}
	}

	ctx := mapping.NewContext(testEventForStore(), nil, false)
	out, err := cfg.Evaluate(ctx)
	if err != nil {
		t.Fatalf("Evaluate must not fail because of a sourceless rule: %v", err)
	}
	if _, present := out["unmapped_field"]; present {
		t.Errorf("unmapped_field should be absent from the evaluated output, got %v", out["unmapped_field"])
	}
}

func testEventForStore() *event.BrowserEvent {
	return &event.BrowserEvent{
		PartyID:   event.DivolteIdentifier{TimestampMillis: 1, ID: "party"},
		SessionID: event.DivolteIdentifier{TimestampMillis: 2, ID: "session"},
	}
}
