package adminui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/example/divolte-rewrite/internal/store"
)

// TestURIPrefixAppearsInRenderedLinksAndRedirects locks in the behavior a
// reverse proxy stripping a path prefix (e.g. HAProxy routing /admin/* here
// with the prefix removed before forwarding) depends on: every root-relative
// link/action/src the server renders, and every redirect Location it sends,
// must have URIPrefix prepended - otherwise a browser resolves those against
// the site root instead of back through the proxy's prefix, breaking
// navigation the moment a link is clicked.
func TestURIPrefixAppearsInRenderedLinksAndRedirects(t *testing.T) {
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
		URIPrefix: "/admin",
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ts := httptest.NewServer(handler)
	defer ts.Close()
	client := newAuthedClient(t)

	// list.html: logo, new-field link, publish form action all carry the
	// prefix.
	resp := doAuthed(t, client, ts, "GET", "/", nil)
	body := readBody(t, resp)
	for _, want := range []string{
		`src="/admin/static/logo.svg"`,
		`href="/admin/fields/new"`,
		`action="/admin/publish"`,
		`action="/admin/fields/reorder"`,
		`admin/fields/set-order`, // html/template's JS-context escaper renders the slashes as \/ - just check the path is present
	} {
		if !strings.Contains(body, want) {
			t.Errorf("list.html missing %q in rendered output", want)
		}
	}

	// form.html: back-link and the create-form action.
	resp = doAuthed(t, client, ts, "GET", "/fields/new", nil)
	body = readBody(t, resp)
	for _, want := range []string{
		`href="/admin/"`,
		`action="/admin/fields"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("form.html missing %q in rendered output", want)
		}
	}

	// A redirect (e.g. after create) must also carry the prefix, or the
	// browser's next request goes to the site root instead of back through
	// the proxy.
	resp = doAuthed(t, client, ts, "POST", "/fields", map[string][]string{
		"name": {"sessionId"}, "base_type": {"string"}, "source_kind": {"builtin"}, "builtin": {"sessionId"},
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("create status = %d, want 303", resp.StatusCode)
	}
	loc, err := resp.Location()
	if err != nil {
		t.Fatalf("Location: %v", err)
	}
	if !strings.HasPrefix(loc.Path, "/admin/") {
		t.Errorf("redirect Location %q does not carry the URI prefix", loc.Path)
	}
}
