package ldapauth

import "testing"

func TestNormalizeServerAddr(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"ldap1.example.com", "ldap://ldap1.example.com"},
		{"  ldap1.example.com  ", "ldap://ldap1.example.com"},
		{"ldap1.example.com:389", "ldap://ldap1.example.com:389"},
		{"ldap://ldap1.example.com", "ldap://ldap1.example.com"},
		{"ldaps://ldap1.example.com", "ldaps://ldap1.example.com"},
	}
	for _, c := range cases {
		if got := normalizeServerAddr(c.in); got != c.want {
			t.Errorf("normalizeServerAddr(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
