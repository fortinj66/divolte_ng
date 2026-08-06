package adminui

import (
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	_ "github.com/go-sql-driver/mysql"

	"github.com/example/divolte-rewrite/internal/avroenc"
	"github.com/example/divolte-rewrite/internal/mapping"
	"github.com/example/divolte-rewrite/internal/store"
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

// newTestStore gives each test its own throwaway database (created via a
// root connection, dropped on cleanup) against the shared dev MariaDB.
func newTestStore(t *testing.T) *store.Store {
	t.Helper()

	dbName := "divolte_test_adminui_" + testDBNameRe.ReplaceAllString(t.Name(), "_")
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

	s, err := store.Open(store.Config{
		Host:     testDBHost,
		Port:     testDBPort,
		Name:     dbName,
		Username: testDBRootUser,
		Password: testDBRootPass,
	})
	if err != nil {
		t.Fatalf("store.Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

type fakePublisher struct {
	calls       int
	lastMapping *mapping.Config
	lastCodec   *avroenc.Codec
}

func (f *fakePublisher) Publish(mappingCfg *mapping.Config, codec *avroenc.Codec) {
	f.calls++
	f.lastMapping = mappingCfg
	f.lastCodec = codec
}

func newTestUI(t *testing.T) (http.Handler, *store.Store, *fakePublisher) {
	t.Helper()
	s := newTestStore(t)

	if err := s.UpsertField(
		store.SchemaField{Name: "partyId", TypeJSON: `"string"`},
		store.MappingRule{Builtin: "partyId"},
	); err != nil {
		t.Fatalf("seeding field: %v", err)
	}

	if err := s.EnsureAdminSettingsSeeded("admin", "secret"); err != nil {
		t.Fatalf("seeding admin settings: %v", err)
	}

	pub := &fakePublisher{}
	handler, err := New(Config{
		Store: s, Publisher: pub,
		SchemaNamespace: "test.record", SchemaRecordName: "trimmed",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return handler, s, pub
}

// newAuthedClient returns an http.Client with a cookie jar - required so
// the CSRF cookie csrfProtect sets on the first request in a test is still
// present (and gets attached automatically) on later requests to the same
// httptest.Server, matching how a real browser session behaves.
func newAuthedClient(t *testing.T) *http.Client {
	t.Helper()
	jar, err := cookiejar.New(nil)
	if err != nil {
		t.Fatalf("cookiejar.New: %v", err)
	}
	return &http.Client{
		Jar:           jar,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}

// csrfTokenFor returns the csrf_token cookie value client currently holds
// for ts, fetching it via an authed GET / first if the jar doesn't have one
// yet (mirroring how a real admin's browser would pick one up on first
// visiting any page before submitting a form).
func csrfTokenFor(t *testing.T, client *http.Client, ts *httptest.Server) string {
	t.Helper()
	u, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("parsing %s: %v", ts.URL, err)
	}
	for _, ck := range client.Jar.Cookies(u) {
		if ck.Name == csrfCookieName {
			return ck.Value
		}
	}
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.SetBasicAuth("admin", "secret")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("priming GET /: %v", err)
	}
	resp.Body.Close()
	for _, ck := range client.Jar.Cookies(u) {
		if ck.Name == csrfCookieName {
			return ck.Value
		}
	}
	t.Fatalf("no %s cookie set after GET /", csrfCookieName)
	return ""
}

func doAuthed(t *testing.T, client *http.Client, ts *httptest.Server, method, path string, body url.Values) *http.Response {
	t.Helper()
	if method == http.MethodPost {
		token := csrfTokenFor(t, client, ts)
		if body == nil {
			body = url.Values{}
		} else {
			cloned := url.Values{}
			for k, v := range body {
				cloned[k] = v
			}
			body = cloned
		}
		body.Set(csrfFormField, token)
	}

	var req *http.Request
	var err error
	if body != nil {
		req, err = http.NewRequest(method, ts.URL+path, strings.NewReader(body.Encode()))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	} else {
		req, err = http.NewRequest(method, ts.URL+path, nil)
	}
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.SetBasicAuth("admin", "secret")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func TestListRequiresBasicAuth(t *testing.T) {
	handler, _, _ := newTestUI(t)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/")
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 without credentials", resp.StatusCode)
	}
}

func TestListShowsSeededField(t *testing.T) {
	handler, _, _ := newTestUI(t)
	ts := httptest.NewServer(handler)
	client := newAuthedClient(t)
	defer ts.Close()

	resp := doAuthed(t, client, ts, "GET", "/", nil)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestCreateEditDeleteField(t *testing.T) {
	handler, s, _ := newTestUI(t)
	ts := httptest.NewServer(handler)
	client := newAuthedClient(t)
	defer ts.Close()

	// Create
	resp := doAuthed(t, client, ts, "POST", "/fields", url.Values{
		"name": {"quantity"}, "base_type": {"int"}, "is_nullable": {"on"}, "default_mode": {"value"}, "default_text": {"0"},
		"source_kind": {"event_param"}, "event_param": {"quantity"}, "coerce": {"int32"},
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("create status = %d, want 303", resp.StatusCode)
	}
	f, r, err := s.GetField("quantity")
	if err != nil || f == nil || r == nil {
		t.Fatalf("GetField(quantity) = %+v, %+v, %v", f, r, err)
	}
	if f.TypeJSON != `["int","null"]` || f.DefaultJSON != "0" {
		t.Errorf("field = %+v", f)
	}
	if r.EventParam != "quantity" || r.Coerce != "int32" {
		t.Errorf("rule = %+v", r)
	}

	// Edit
	resp = doAuthed(t, client, ts, "POST", "/fields/quantity", url.Values{
		"base_type": {"int"}, "is_nullable": {"on"}, "default_mode": {"value"}, "default_text": {"5"},
		"source_kind": {"event_param"}, "event_param": {"qty"}, "coerce": {"int32"},
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("edit status = %d, want 303", resp.StatusCode)
	}
	f, r, err = s.GetField("quantity")
	if err != nil || f.DefaultJSON != "5" || r.EventParam != "qty" {
		t.Fatalf("after edit: field=%+v rule=%+v err=%v", f, r, err)
	}

	// Delete
	resp = doAuthed(t, client, ts, "POST", "/fields/quantity/delete", url.Values{})
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("delete status = %d, want 303", resp.StatusCode)
	}
	f, _, err = s.GetField("quantity")
	if err != nil || f != nil {
		t.Errorf("expected field to be deleted, got %+v (err=%v)", f, err)
	}
}

func TestReorderMovesConsecutiveFields(t *testing.T) {
	handler, s, _ := newTestUI(t)
	ts := httptest.NewServer(handler)
	client := newAuthedClient(t)
	defer ts.Close()

	for _, name := range []string{"x", "y", "z"} {
		if err := s.UpsertField(store.SchemaField{Name: name, TypeJSON: `"string"`}, store.MappingRule{EventParam: name}); err != nil {
			t.Fatalf("seeding %q: %v", name, err)
		}
	}
	// Order is now: partyId, x, y, z

	resp := doAuthed(t, client, ts, "POST", "/fields/reorder", url.Values{
		"selected_fields": {"y", "z"}, "direction": {"up"},
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("reorder status = %d, want 303", resp.StatusCode)
	}

	fields, err := s.ListSchemaFields()
	if err != nil {
		t.Fatalf("ListSchemaFields: %v", err)
	}
	got := make([]string, len(fields))
	for i, f := range fields {
		got[i] = f.Name
	}
	// "up" swaps the selected block past its one immediate neighbor (x),
	// not all the way to the top - partyId stays put.
	want := []string{"partyId", "y", "z", "x"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("got %v, want %v", got, want)
			break
		}
	}
}

func TestReorderRejectsNonConsecutiveSelectionViaHTTP(t *testing.T) {
	handler, s, _ := newTestUI(t)
	ts := httptest.NewServer(handler)
	client := newAuthedClient(t)
	defer ts.Close()

	for _, name := range []string{"x", "y", "z"} {
		if err := s.UpsertField(store.SchemaField{Name: name, TypeJSON: `"string"`}, store.MappingRule{EventParam: name}); err != nil {
			t.Fatalf("seeding %q: %v", name, err)
		}
	}

	resp := doAuthed(t, client, ts, "POST", "/fields/reorder", url.Values{
		"selected_fields": {"partyId", "y"}, "direction": {"up"},
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("status = %d, want 303 (redirect with error flash)", resp.StatusCode)
	}
	loc, err := resp.Location()
	if err != nil {
		t.Fatalf("Location: %v", err)
	}
	if loc.Query().Get("flash_class") != "err" {
		t.Errorf("expected an error flash, got query %q", loc.RawQuery)
	}
}

func TestPublishInvokesPublisherWithRebuiltConfig(t *testing.T) {
	handler, _, pub := newTestUI(t)
	ts := httptest.NewServer(handler)
	client := newAuthedClient(t)
	defer ts.Close()

	resp := doAuthed(t, client, ts, "POST", "/publish", url.Values{})
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("publish status = %d, want 303", resp.StatusCode)
	}
	if pub.calls != 1 {
		t.Fatalf("Publish called %d times, want 1", pub.calls)
	}
	if len(pub.lastMapping.Fields) != 1 || pub.lastMapping.Fields[0].Field != "partyId" {
		t.Errorf("published mapping = %+v", pub.lastMapping.Fields)
	}
	if pub.lastCodec == nil {
		t.Error("published codec is nil")
	}
}

func TestSetOrderAppliesDragDropOrder(t *testing.T) {
	handler, s, _ := newTestUI(t)
	ts := httptest.NewServer(handler)
	client := newAuthedClient(t)
	defer ts.Close()

	for _, name := range []string{"x", "y"} {
		if err := s.UpsertField(store.SchemaField{Name: name, TypeJSON: `"string"`}, store.MappingRule{EventParam: name}); err != nil {
			t.Fatalf("seeding %q: %v", name, err)
		}
	}
	// Order is now: partyId, x, y

	resp := doAuthed(t, client, ts, "POST", "/fields/set-order", url.Values{
		"field_order": {"y", "partyId", "x"},
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}

	fields, err := s.ListSchemaFields()
	if err != nil {
		t.Fatalf("ListSchemaFields: %v", err)
	}
	got := make([]string, len(fields))
	for i, f := range fields {
		got[i] = f.Name
	}
	want := []string{"y", "partyId", "x"}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] || got[2] != want[2] {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestSetOrderRejectsInvalidOrder(t *testing.T) {
	handler, _, _ := newTestUI(t)
	ts := httptest.NewServer(handler)
	client := newAuthedClient(t)
	defer ts.Close()

	resp := doAuthed(t, client, ts, "POST", "/fields/set-order", url.Values{
		"field_order": {"nonexistent"},
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestListShowsRevertButtonOnlyWithUnpublishedChanges(t *testing.T) {
	handler, s, _ := newTestUI(t)
	ts := httptest.NewServer(handler)
	client := newAuthedClient(t)
	defer ts.Close()

	// Before any snapshot: the seeded field counts as unpublished.
	resp := doAuthed(t, client, ts, "GET", "/", nil)
	body := readBody(t, resp)
	if !strings.Contains(body, "Revert changes") {
		t.Error("expected Revert button before any publish, since the seeded field is unpublished")
	}

	if err := s.SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	resp = doAuthed(t, client, ts, "GET", "/", nil)
	body = readBody(t, resp)
	if strings.Contains(body, "Revert changes") {
		t.Error("expected no Revert button right after a snapshot with no further changes")
	}

	if err := s.UpsertField(store.SchemaField{Name: "x", TypeJSON: `"string"`}, store.MappingRule{EventParam: "x"}); err != nil {
		t.Fatalf("UpsertField: %v", err)
	}
	resp = doAuthed(t, client, ts, "GET", "/", nil)
	body = readBody(t, resp)
	if !strings.Contains(body, "Revert changes") {
		t.Error("expected Revert button after adding a field post-snapshot")
	}
}

func TestRevertRestoresSnapshotViaHTTP(t *testing.T) {
	handler, s, _ := newTestUI(t)
	ts := httptest.NewServer(handler)
	client := newAuthedClient(t)
	defer ts.Close()

	if err := s.SaveSnapshot(); err != nil {
		t.Fatalf("SaveSnapshot: %v", err)
	}
	if err := s.DeleteField("partyId"); err != nil {
		t.Fatalf("DeleteField: %v", err)
	}
	if f, _, _ := s.GetField("partyId"); f != nil {
		t.Fatal("partyId should be deleted before revert")
	}

	resp := doAuthed(t, client, ts, "POST", "/revert", url.Values{})
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("status = %d, want 303", resp.StatusCode)
	}

	f, _, err := s.GetField("partyId")
	if err != nil {
		t.Fatalf("GetField: %v", err)
	}
	if f == nil {
		t.Error("expected partyId to be restored by revert")
	}
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	buf := new(strings.Builder)
	if _, err := io.Copy(buf, resp.Body); err != nil {
		t.Fatalf("reading response body: %v", err)
	}
	return buf.String()
}
