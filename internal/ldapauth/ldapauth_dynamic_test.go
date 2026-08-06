package ldapauth

import (
	"errors"
	"testing"
)

func TestDynamicAuthenticatorDeniesWhenDisabled(t *testing.T) {
	calls := 0
	d := NewDynamic(func() (Config, bool, error) {
		calls++
		return Config{}, false, nil
	})
	ok, err := d.Authenticate("jsmith", "hunter2")
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	if ok {
		t.Error("Authenticate should deny when configFn reports disabled")
	}
	if calls != 1 {
		t.Errorf("configFn called %d times, want 1", calls)
	}
}

func TestDynamicAuthenticatorPropagatesConfigFnError(t *testing.T) {
	wantErr := errors.New("database unreachable")
	d := NewDynamic(func() (Config, bool, error) {
		return Config{}, true, wantErr
	})
	_, err := d.Authenticate("jsmith", "hunter2")
	if err == nil {
		t.Fatal("Authenticate should return an error when configFn errors")
	}
}

func TestDynamicAuthenticatorRejectsInvalidStoredConfig(t *testing.T) {
	// Enabled=true but no AllowedGroups - New() must refuse this the same
	// way it would for a static Authenticator; a partially-filled-in
	// settings form must not silently grant unrestricted access.
	d := NewDynamic(func() (Config, bool, error) {
		return Config{
			Servers:          []string{"ldap://example.invalid"},
			UserSearchBase:   "DC=example,DC=com",
			UserSearchFilter: "sAMAccountName={0}",
			ManagerDN:        "CN=svc,DC=example,DC=com",
			ManagerPassword:  "secret",
		}, true, nil
	})
	_, err := d.Authenticate("jsmith", "hunter2")
	if err == nil {
		t.Fatal("Authenticate should error on a stored config with no AllowedGroups")
	}
}
