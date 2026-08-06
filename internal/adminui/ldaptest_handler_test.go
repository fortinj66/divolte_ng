package adminui

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
)

func newTestUIWithLDAPTest(t *testing.T, testFn LDAPTestFunc) http.Handler {
	t.Helper()
	s := newTestStore(t)
	if err := s.EnsureAdminSettingsSeeded("admin", "secret"); err != nil {
		t.Fatalf("seeding admin settings: %v", err)
	}
	handler, err := New(Config{
		Store: s, Publisher: &fakePublisher{},
		SchemaNamespace: "test.record", SchemaRecordName: "trimmed",
		LDAPTest: testFn,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return handler
}

// TestSettingsTestLDAPReturnsHandlerResultInline confirms the Test button's
// endpoint reports the LDAPTest func's result as JSON without redirecting
// (unlike the real settingsUpdate save, which does redirect) - the
// settings.html JS relies on this to show the result without a page
// reload.
func TestSettingsTestLDAPReturnsHandlerResultInline(t *testing.T) {
	var gotServers []string
	var gotGroups []string
	handler := newTestUIWithLDAPTest(t, func(servers []string, managerDN, managerPassword, userSearchBase, userSearchFilter string, allowedGroups []string) (string, error) {
		gotServers = servers
		gotGroups = allowedGroups
		return "connected fine", nil
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()
	client := newAuthedClient(t)

	resp := doAuthed(t, client, ts, "POST", "/settings/test-ldap", url.Values{
		"ldap_servers":            {"ldap://a.example.com\nldap://b.example.com"},
		"ldap_manager_dn":         {"CN=svc,DC=example,DC=com"},
		"ldap_manager_password":   {"secretpw"},
		"ldap_user_search_base":   {"DC=example,DC=com"},
		"ldap_user_search_filter": {"sAMAccountName={0}"},
		"ldap_allowed_groups":     {"admins\nother-group"},
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if len(gotServers) != 2 || gotServers[0] != "ldap://a.example.com" || gotServers[1] != "ldap://b.example.com" {
		t.Errorf("servers passed to LDAPTest = %v, want two split lines", gotServers)
	}
	if len(gotGroups) != 2 || gotGroups[0] != "admins" || gotGroups[1] != "other-group" {
		t.Errorf("groups passed to LDAPTest = %v, want two split lines", gotGroups)
	}
}

// TestSettingsTestLDAPFallsBackToStoredPasswordWhenBlank confirms a blank
// ldap_manager_password field (the "keep current password" convention
// used by the real save, too) doesn't get tested as an empty password -
// it falls back to whatever's already saved.
func TestSettingsTestLDAPFallsBackToStoredPasswordWhenBlank(t *testing.T) {
	var gotPassword string
	handler := newTestUIWithLDAPTest(t, func(servers []string, managerDN, managerPassword, userSearchBase, userSearchFilter string, allowedGroups []string) (string, error) {
		gotPassword = managerPassword
		return "ok", nil
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()
	client := newAuthedClient(t)

	// First, save real LDAP settings with a known password via the normal
	// settings form.
	saveResp := doAuthed(t, client, ts, "POST", "/settings", url.Values{
		"username": {"admin"}, "password": {""}, "primary_url": {""},
		"ldap_enabled": {"on"}, "ldap_servers": {"ldap://a.example.com"},
		"ldap_manager_dn": {"CN=svc,DC=example,DC=com"}, "ldap_manager_password": {"storedpw"},
		"ldap_user_search_base": {"DC=example,DC=com"}, "ldap_user_search_filter": {"sAMAccountName={0}"},
		"ldap_allowed_groups": {"admins"},
	})
	saveResp.Body.Close()
	if saveResp.StatusCode != http.StatusSeeOther {
		t.Fatalf("saving settings status = %d, want 303", saveResp.StatusCode)
	}

	// Now test with a BLANK manager password field.
	testResp := doAuthed(t, client, ts, "POST", "/settings/test-ldap", url.Values{
		"ldap_servers": {"ldap://a.example.com"}, "ldap_manager_dn": {"CN=svc,DC=example,DC=com"},
		"ldap_manager_password": {""}, "ldap_user_search_base": {"DC=example,DC=com"},
		"ldap_user_search_filter": {"sAMAccountName={0}"}, "ldap_allowed_groups": {"admins"},
	})
	defer testResp.Body.Close()
	if gotPassword != "storedpw" {
		t.Errorf("LDAPTest received password %q, want the stored password %q", gotPassword, "storedpw")
	}
}

// TestSettingsTestLDAPWithoutTestFuncConfigured confirms a build with no
// LDAPTest wired up (LDAPTest is nil) responds gracefully, not a panic or
// bare 404/500.
func TestSettingsTestLDAPWithoutTestFuncConfigured(t *testing.T) {
	handler := newTestUIWithLDAPTest(t, nil)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	client := newAuthedClient(t)

	resp := doAuthed(t, client, ts, "POST", "/settings/test-ldap", url.Values{})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200 with a graceful message body", resp.StatusCode)
	}
	body := readBody(t, resp)
	if !strings.Contains(body, "not available") {
		t.Errorf("body = %q, want a message indicating LDAP testing isn't available", body)
	}
}
