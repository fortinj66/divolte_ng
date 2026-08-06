package ldapauth

import "testing"

// These tests cover the pure logic (filter building, DN detection, and
// New's validation) without a live directory - this environment has no
// test AD instance to safely exercise the actual search-then-bind flow
// against, so Authenticate itself isn't covered here. Verify that path
// manually against a real (non-production) AD account before relying on
// it in production.

func TestNewRequiresAllowedGroups(t *testing.T) {
	_, err := New(Config{
		Servers:          []string{"ldap://example.invalid"},
		UserSearchBase:   "DC=example,DC=com",
		UserSearchFilter: "sAMAccountName={0}",
		ManagerDN:        "CN=svc,DC=example,DC=com",
		ManagerPassword:  "secret",
		// AllowedGroups deliberately omitted.
	})
	if err == nil {
		t.Fatal("New with no AllowedGroups should error - this package must never authenticate without a group restriction")
	}
}

func TestNewRequiresServers(t *testing.T) {
	_, err := New(Config{
		UserSearchBase:   "DC=example,DC=com",
		UserSearchFilter: "sAMAccountName={0}",
		ManagerDN:        "CN=svc,DC=example,DC=com",
		ManagerPassword:  "secret",
		AllowedGroups:    []string{"admins"},
	})
	if err == nil {
		t.Fatal("New with no Servers should error")
	}
}

func TestNewRequiresManagerCredentials(t *testing.T) {
	base := Config{
		Servers:          []string{"ldap://example.invalid"},
		UserSearchBase:   "DC=example,DC=com",
		UserSearchFilter: "sAMAccountName={0}",
		AllowedGroups:    []string{"admins"},
	}
	if _, err := New(base); err == nil {
		t.Error("New with no ManagerDN/ManagerPassword should error")
	}
	withDN := base
	withDN.ManagerDN = "CN=svc,DC=example,DC=com"
	if _, err := New(withDN); err == nil {
		t.Error("New with ManagerDN but no ManagerPassword should error")
	}
}

func TestNewSucceedsWithValidConfig(t *testing.T) {
	_, err := New(Config{
		Servers:          []string{"ldap://example.invalid"},
		UserSearchBase:   "DC=example,DC=com",
		UserSearchFilter: "sAMAccountName={0}",
		ManagerDN:        "CN=svc,DC=example,DC=com",
		ManagerPassword:  "secret",
		AllowedGroups:    []string{"admins"},
	})
	if err != nil {
		t.Fatalf("New with a valid config should succeed, got: %v", err)
	}
}

func TestBuildUserSearchFilterSubstitutesAndEscapes(t *testing.T) {
	// A bare "sAMAccountName={0}" (no wrapping parens) is a natural thing
	// to type into a field labeled "User search filter" - go-ldap
	// requires the parens, so this must come out wrapped rather than
	// erroring at query time (a real failure mode: an admin entered
	// exactly this value and every login silently failed to compile the
	// filter until this was fixed).
	got := buildUserSearchFilter("sAMAccountName={0}", "jsmith")
	want := "(sAMAccountName=jsmith)"
	if got != want {
		t.Errorf("buildUserSearchFilter = %q, want %q", got, want)
	}

	// An already-wrapped filter is left alone (not double-wrapped).
	got = buildUserSearchFilter("(sAMAccountName={0})", "jsmith")
	if got != want {
		t.Errorf("buildUserSearchFilter (already wrapped) = %q, want %q", got, want)
	}

	// A username containing a filter metacharacter must come out escaped,
	// not substituted raw - otherwise a crafted username could inject
	// extra filter clauses (LDAP filter injection).
	got = buildUserSearchFilter("sAMAccountName={0}", "jsmith)(uid=*")
	if got == "(sAMAccountName=jsmith)(uid=*)" {
		t.Errorf("buildUserSearchFilter did not escape filter metacharacters: %q", got)
	}
}

func TestEnsureParenWrapped(t *testing.T) {
	cases := []struct{ in, want string }{
		{"sAMAccountName={0}", "(sAMAccountName={0})"},
		{"  sAMAccountName={0}  ", "(sAMAccountName={0})"},
		{"(sAMAccountName={0})", "(sAMAccountName={0})"},
		{"(&(objectClass=user)(sAMAccountName={0}))", "(&(objectClass=user)(sAMAccountName={0}))"},
	}
	for _, c := range cases {
		if got := ensureParenWrapped(c.in); got != c.want {
			t.Errorf("ensureParenWrapped(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestBuildNestedGroupFilterUsesMatchingRuleInChain(t *testing.T) {
	got := buildNestedGroupFilter("CN=admins,OU=Groups,DC=example,DC=com")
	want := "(memberOf:1.2.840.113556.1.4.1941:=CN=admins,OU=Groups,DC=example,DC=com)"
	if got != want {
		t.Errorf("buildNestedGroupFilter = %q, want %q", got, want)
	}
}

func TestIsDN(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"admins", false},
		{"CN=admins,OU=Groups,DC=example,DC=com", true},
		{"", false},
	}
	for _, c := range cases {
		if got := isDN(c.in); got != c.want {
			t.Errorf("isDN(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}
