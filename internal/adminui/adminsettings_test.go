package adminui

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

// TestSettingsUpdateChangesSharedCredentials confirms /settings rewrites the
// shared admin_settings row (not just this instance's own view of it) - the
// old login must stop working and the new one must work, since every
// Divolte instance authenticates against the same database row.
func TestSettingsUpdateChangesSharedCredentials(t *testing.T) {
	handler, _, _ := newTestUI(t)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	client := newAuthedClient(t)

	resp := doAuthed(t, client, ts, "POST", "/settings", url.Values{
		"username": {"newadmin"}, "password": {"newsecret"}, "primary_url": {""},
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("settings update status = %d, want 303", resp.StatusCode)
	}

	// Old credentials must no longer work.
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	req.SetBasicAuth("admin", "secret")
	oldResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET / with old creds: %v", err)
	}
	oldResp.Body.Close()
	if oldResp.StatusCode != http.StatusUnauthorized {
		t.Errorf("old credentials status = %d, want 401", oldResp.StatusCode)
	}

	// New credentials must work.
	req2, _ := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	req2.SetBasicAuth("newadmin", "newsecret")
	newResp, err := http.DefaultClient.Do(req2)
	if err != nil {
		t.Fatalf("GET / with new creds: %v", err)
	}
	newResp.Body.Close()
	if newResp.StatusCode != http.StatusOK {
		t.Errorf("new credentials status = %d, want 200", newResp.StatusCode)
	}
}

// TestSettingsPasswordBlankKeepsCurrentPassword confirms submitting the
// settings form with an empty password field doesn't wipe the password -
// the form never round-trips the current password back into the field, so
// blank must mean "no change", not "set to empty".
func TestSettingsPasswordBlankKeepsCurrentPassword(t *testing.T) {
	handler, _, _ := newTestUI(t)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	client := newAuthedClient(t)

	resp := doAuthed(t, client, ts, "POST", "/settings", url.Values{
		"username": {"admin"}, "password": {""}, "primary_url": {""},
	})
	resp.Body.Close()
	if resp.StatusCode != http.StatusSeeOther {
		t.Fatalf("settings update status = %d, want 303", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	req.SetBasicAuth("admin", "secret")
	stillResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET / with original password: %v", err)
	}
	stillResp.Body.Close()
	if stillResp.StatusCode != http.StatusOK {
		t.Errorf("original password status = %d, want 200 (blank password field must not change it)", stillResp.StatusCode)
	}
}

// TestPrimaryRedirectSendsToStoredPrimary confirms that once a primary URL
// is set, hitting this instance redirects to it (path/query preserved) -
// unless the request's own Host already matches the primary, which must be
// a no-op rather than a pointless (or looping) redirect to itself.
func TestPrimaryRedirectSendsToStoredPrimary(t *testing.T) {
	handler, s, _ := newTestUI(t)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	client := newAuthedClient(t)

	if err := s.SetPrimaryURL("https://collector-01.example.com/admin"); err != nil {
		t.Fatalf("SetPrimaryURL: %v", err)
	}

	resp := doAuthed(t, client, ts, "GET", "/fields/new", nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusFound {
		t.Fatalf("status = %d, want 302 redirect to the stored primary", resp.StatusCode)
	}
	loc, err := resp.Location()
	if err != nil {
		t.Fatalf("Location: %v", err)
	}
	want := "https://collector-01.example.com/admin/fields/new"
	if loc.String() != want {
		t.Errorf("redirect Location = %q, want %q", loc.String(), want)
	}

	// /settings itself must always be reachable directly, even when a
	// (possibly wrong) primary is set - otherwise a bad value would strand
	// every instance with no way to fix it back through the UI.
	settingsResp := doAuthed(t, client, ts, "GET", "/settings", nil)
	settingsResp.Body.Close()
	if settingsResp.StatusCode != http.StatusOK {
		t.Errorf("/settings status = %d, want 200 even with a primary URL set", settingsResp.StatusCode)
	}
}

// TestPrimaryRedirectNoOpWhenAlreadyAtPrimary confirms that when the
// stored primary URL's host matches the current request's own Host, the
// request is served normally instead of redirecting to itself.
func TestPrimaryRedirectNoOpWhenAlreadyAtPrimary(t *testing.T) {
	handler, s, _ := newTestUI(t)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	client := newAuthedClient(t)

	// ts.URL's host IS "the primary" from this instance's own point of view.
	tsHost := strings.TrimPrefix(strings.TrimPrefix(ts.URL, "http://"), "https://")
	if err := s.SetPrimaryURL("http://" + tsHost + "/"); err != nil {
		t.Fatalf("SetPrimaryURL: %v", err)
	}

	resp := doAuthed(t, client, ts, "GET", "/", nil)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (no redirect when already at the primary host)", resp.StatusCode)
	}
}
