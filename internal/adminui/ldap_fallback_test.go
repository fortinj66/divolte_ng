package adminui

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
)

// fakeLDAPAuth lets tests control exactly what Authenticate returns
// without a real directory - see internal/ldapauth's own tests for the
// pure-logic coverage (filter building, New's validation) that doesn't
// need a fake at all.
type fakeLDAPAuth struct {
	validUser, validPass string
	err                  error
	calls                int
}

func (f *fakeLDAPAuth) Authenticate(user, pass string) (bool, error) {
	f.calls++
	if f.err != nil {
		return false, f.err
	}
	return user == f.validUser && pass == f.validPass, nil
}

func newTestUIWithLDAP(t *testing.T, ldapAuth LDAPAuthenticator) http.Handler {
	t.Helper()
	s := newTestStore(t)
	if err := s.EnsureAdminSettingsSeeded("admin", "secret"); err != nil {
		t.Fatalf("seeding admin settings: %v", err)
	}
	handler, err := New(Config{
		Store: s, Publisher: &fakePublisher{},
		SchemaNamespace: "test.record", SchemaRecordName: "trimmed",
		LDAPAuth: ldapAuth,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return handler
}

// TestBasicAuthFallsBackToLDAPWhenDBCredentialsDontMatch confirms an LDAP
// login that doesn't match the shared database credentials still grants
// access - the two methods are alternatives, not a replacement.
func TestBasicAuthFallsBackToLDAPWhenDBCredentialsDontMatch(t *testing.T) {
	ldapAuth := &fakeLDAPAuth{validUser: "jsmith", validPass: "hunter2"}
	handler := newTestUIWithLDAP(t, ldapAuth)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	req.SetBasicAuth("jsmith", "hunter2")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200 (valid LDAP login should grant access)", resp.StatusCode)
	}
	if ldapAuth.calls != 1 {
		t.Errorf("LDAP Authenticate called %d times, want 1", ldapAuth.calls)
	}
}

// TestBasicAuthDoesNotConsultLDAPWhenDBCredentialsMatch confirms the
// database check is tried first and short-circuits - no need to hit LDAP
// (and no way to test-observe its latency/availability) when the shared
// login already matches.
func TestBasicAuthDoesNotConsultLDAPWhenDBCredentialsMatch(t *testing.T) {
	ldapAuth := &fakeLDAPAuth{validUser: "jsmith", validPass: "hunter2"}
	handler := newTestUIWithLDAP(t, ldapAuth)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	req.SetBasicAuth("admin", "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
	if ldapAuth.calls != 0 {
		t.Errorf("LDAP Authenticate called %d times, want 0 (database match should short-circuit)", ldapAuth.calls)
	}
}

// TestBasicAuthRejectsWhenNeitherMethodMatches confirms failing both
// checks still yields a normal 401, not a 500 or an information leak
// about which method almost worked.
func TestBasicAuthRejectsWhenNeitherMethodMatches(t *testing.T) {
	ldapAuth := &fakeLDAPAuth{validUser: "jsmith", validPass: "hunter2"}
	handler := newTestUIWithLDAP(t, ldapAuth)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	req.SetBasicAuth("nobody", "wrong")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", resp.StatusCode)
	}
}

// TestBasicAuthTreatsLDAPErrorAsDenial confirms an LDAP connection/search
// failure fails closed (401), not open (200) and not a 500 that would
// leak internal error detail to an unauthenticated caller.
func TestBasicAuthTreatsLDAPErrorAsDenial(t *testing.T) {
	ldapAuth := &fakeLDAPAuth{err: errors.New("connection refused")}
	handler := newTestUIWithLDAP(t, ldapAuth)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	req.SetBasicAuth("jsmith", "hunter2")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401 (LDAP errors must fail closed)", resp.StatusCode)
	}
}

// TestBasicAuthWorksWithNilLDAPAuth confirms every existing deployment
// (no LDAPAuth configured) is completely unaffected - only the database
// login works, same as before this feature existed.
func TestBasicAuthWorksWithNilLDAPAuth(t *testing.T) {
	handler := newTestUIWithLDAP(t, nil)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/", nil)
	req.SetBasicAuth("admin", "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}
