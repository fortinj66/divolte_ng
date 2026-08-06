package store

import (
	"reflect"
	"testing"
)

func TestGetLDAPSettingsBeforeSeedingReturnsZeroValueNotError(t *testing.T) {
	s := openTestStore(t)
	got, err := s.GetLDAPSettings()
	if err != nil {
		t.Fatalf("GetLDAPSettings: %v", err)
	}
	if got.Enabled {
		t.Errorf("GetLDAPSettings before any seed/set should report Enabled=false, got %+v", got)
	}
}

func TestEnsureLDAPSettingsSeededIsNoOpAfterFirstCall(t *testing.T) {
	s := openTestStore(t)
	first := LDAPSettings{
		Enabled: true, Servers: []string{"ldap://a.example.com"},
		ManagerDN: "CN=svc,DC=example,DC=com", ManagerPassword: "p1",
		UserSearchBase: "DC=example,DC=com", UserSearchFilter: "sAMAccountName={0}",
		AllowedGroups: []string{"admins"},
	}
	if err := s.EnsureLDAPSettingsSeeded(first); err != nil {
		t.Fatalf("EnsureLDAPSettingsSeeded (first): %v", err)
	}

	// A second seed call with DIFFERENT values must not overwrite the
	// first - matching EnsureAdminSettingsSeeded's "only the first
	// instance's values stick" contract, since every instance calls this
	// on its own startup.
	second := first
	second.Servers = []string{"ldap://b.example.com"}
	second.ManagerPassword = "p2"
	if err := s.EnsureLDAPSettingsSeeded(second); err != nil {
		t.Fatalf("EnsureLDAPSettingsSeeded (second): %v", err)
	}

	got, err := s.GetLDAPSettings()
	if err != nil {
		t.Fatalf("GetLDAPSettings: %v", err)
	}
	if !reflect.DeepEqual(got, first) {
		t.Errorf("GetLDAPSettings = %+v, want the FIRST seed's values %+v (second seed should be a no-op)", got, first)
	}
}

func TestSetLDAPSettingsRoundTripsAndReplacesWholesale(t *testing.T) {
	s := openTestStore(t)
	want := LDAPSettings{
		Enabled:   true,
		Servers:   []string{"ldap://ldap1.example.com", "ldap://ldap2.example.com"},
		ManagerDN: "CN=svc-account,OU=Service Accounts,DC=example,DC=com", ManagerPassword: "hunter2",
		UserSearchBase: "DC=example,DC=com", UserSearchFilter: "sAMAccountName={0}",
		AllowedGroups: []string{"admins", "CN=other,OU=Groups,DC=example,DC=com"},
	}
	if err := s.SetLDAPSettings(want); err != nil {
		t.Fatalf("SetLDAPSettings: %v", err)
	}
	got, err := s.GetLDAPSettings()
	if err != nil {
		t.Fatalf("GetLDAPSettings: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetLDAPSettings = %+v, want %+v", got, want)
	}

	// SetLDAPSettings works as an upsert even without a prior seed/set -
	// re-verify by fully replacing with a second, disjoint set of values.
	replacement := LDAPSettings{
		Enabled: false, Servers: nil,
		ManagerDN: "", ManagerPassword: "",
		UserSearchBase: "", UserSearchFilter: "",
		AllowedGroups: nil,
	}
	if err := s.SetLDAPSettings(replacement); err != nil {
		t.Fatalf("SetLDAPSettings (replacement): %v", err)
	}
	got, err = s.GetLDAPSettings()
	if err != nil {
		t.Fatalf("GetLDAPSettings after replacement: %v", err)
	}
	if !reflect.DeepEqual(got, replacement) {
		t.Errorf("GetLDAPSettings after replacement = %+v, want %+v (SetLDAPSettings must fully replace, not merge)", got, replacement)
	}
}
