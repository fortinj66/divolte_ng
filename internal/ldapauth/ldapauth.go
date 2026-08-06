// Package ldapauth authenticates admin-UI login attempts against Active
// Directory as an ADDITIONAL method alongside the shared database-backed
// login (internal/store's AdminSettings) - either credential set grants
// access; this package never replaces the database check, only
// supplements it (see internal/adminui's basicAuth).
//
// Uses the standard "search then bind" LDAP pattern, the same one already
// working elsewhere in this environment (NiFi's own login-identity-
// providers.xml, same AD domain controllers): a service account searches
// for the submitted username's DN, then a second connection binds AS
// that DN with the submitted password to actually verify it - AD never
// returns a comparable password hash via search, so a real bind is the
// only way to check a password is correct.
//
// Unlike that NiFi reference config (which accepts any authenticated
// domain account with no group restriction), this package ALWAYS requires
// group membership - the admin UI can edit live schema/mapping and
// publish changes, a much more sensitive write surface than a NiFi login.
// New refuses to construct an Authenticator with an empty AllowedGroups.
package ldapauth

import (
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/go-ldap/ldap/v3"
)

// nestedGroupMatchingRule is Active Directory's OID for matching group
// membership transitively (LDAP_MATCHING_RULE_IN_CHAIN) - without it, a
// memberOf-style filter only sees DIRECT group membership, silently
// missing anyone who belongs via a nested group.
const nestedGroupMatchingRule = "1.2.840.113556.1.4.1941"

// Config configures the LDAP connection and access policy. See
// internal/config's LDAPConfig doc comment for the field-level rationale
// behind each of these - this is a plain copy so this package doesn't
// need to import internal/config, keeping it independently testable and
// free of an import-cycle risk.
type Config struct {
	// Servers are tried in order until one connects, e.g.
	// "ldap://ldap1.example.com".
	Servers []string

	// ManagerDN/ManagerPassword bind to search for the user's DN before
	// the real authentication bind - a service account, not the end
	// user's own credentials.
	ManagerDN       string
	ManagerPassword string

	// UserSearchBase/UserSearchFilter locate the user's DN - "{0}" in the
	// filter is replaced with the submitted username (e.g. base
	// "DC=example,DC=com", filter "sAMAccountName={0}").
	UserSearchBase   string
	UserSearchFilter string

	// AllowedGroups: the submitted user must belong to at least one of
	// these AD groups (nested/indirect membership included) to
	// authenticate. Each entry may be a full distinguished name or a bare
	// group name (e.g. "admins"), resolved to its DN via
	// UserSearchBase at authentication time. Must be non-empty - New
	// errors otherwise.
	AllowedGroups []string

	// ConnectTimeout/ReadTimeout default to 10s each when zero.
	ConnectTimeout time.Duration
	ReadTimeout    time.Duration
}

// Authenticator checks a username/password against AD.
type Authenticator struct {
	cfg Config
}

// New validates cfg and returns an Authenticator. It does not connect to
// any server yet - connections are made per Authenticate call, since a
// login attempt is infrequent enough that pooling isn't worth the
// complexity, and a stale pooled connection failing silently would be a
// worse failure mode than a fresh dial every time.
func New(cfg Config) (*Authenticator, error) {
	if len(cfg.Servers) == 0 {
		return nil, fmt.Errorf("ldapauth: at least one server is required")
	}
	if cfg.UserSearchBase == "" {
		return nil, fmt.Errorf("ldapauth: user search base is required")
	}
	if cfg.UserSearchFilter == "" {
		return nil, fmt.Errorf("ldapauth: user search filter is required")
	}
	if len(cfg.AllowedGroups) == 0 {
		return nil, fmt.Errorf("ldapauth: at least one allowed group is required - this package never authenticates without a group restriction")
	}
	if cfg.ManagerDN == "" {
		return nil, fmt.Errorf("ldapauth: manager DN is required")
	}
	if cfg.ManagerPassword == "" {
		return nil, fmt.Errorf("ldapauth: manager password is required")
	}
	return &Authenticator{cfg: cfg}, nil
}

// Authenticate reports whether username/password are a valid AD login AND
// username belongs to at least one configured AllowedGroups entry. A
// connection or search failure is returned as an error (distinct from
// "authenticated but not authorized" or "no such user", both of which are
// simply (false, nil)) so callers can tell "LDAP is down" apart from
// "wrong credentials" - the caller (internal/adminui's basicAuth) treats
// an error as a denial either way, but logs it, which a plain (false, nil)
// wouldn't surface.
func (a *Authenticator) Authenticate(username, password string) (bool, error) {
	if username == "" || password == "" {
		return false, nil
	}

	conn, err := a.dial()
	if err != nil {
		return false, fmt.Errorf("ldapauth: connecting: %w", err)
	}
	defer conn.Close()

	if err := conn.Bind(a.cfg.ManagerDN, a.cfg.ManagerPassword); err != nil {
		return false, fmt.Errorf("ldapauth: manager bind: %w", err)
	}

	userDN, err := a.findUserDN(conn, username)
	if err != nil {
		return false, err
	}
	if userDN == "" {
		log.Printf("ldapauth: user %q not found under search base %q", username, a.cfg.UserSearchBase)
		return false, nil
	}
	log.Printf("ldapauth: user %q resolved to %s", username, userDN)

	member, err := a.isMemberOfAllowedGroup(conn, userDN)
	if err != nil {
		return false, err
	}
	if !member {
		log.Printf("ldapauth: user %q (%s) is not a member of any configured allowed group %v", username, userDN, a.cfg.AllowedGroups)
		return false, nil
	}
	log.Printf("ldapauth: user %q (%s) is a member of an allowed group, attempting password bind", username, userDN)

	// The manager connection has proven the user exists and belongs to an
	// allowed group, but not that the submitted password is correct - a
	// fresh connection binding as the user's own DN is the only way to
	// verify that.
	userConn, err := a.dial()
	if err != nil {
		return false, fmt.Errorf("ldapauth: connecting for user bind: %w", err)
	}
	defer userConn.Close()

	if err := userConn.Bind(userDN, password); err != nil {
		var ldapErr *ldap.Error
		if errors.As(err, &ldapErr) && ldapErr.ResultCode == ldap.LDAPResultInvalidCredentials {
			log.Printf("ldapauth: password bind for %q (%s) failed: invalid credentials", username, userDN)
			return false, nil
		}
		log.Printf("ldapauth: password bind for %q (%s) failed: %v", username, userDN, err)
		return false, fmt.Errorf("ldapauth: user bind: %w", err)
	}
	log.Printf("ldapauth: user %q (%s) authenticated successfully", username, userDN)
	return true, nil
}

func (a *Authenticator) dial() (*ldap.Conn, error) {
	dialer := &net.Dialer{Timeout: a.connectTimeout()}
	var lastErr error
	for _, addr := range a.cfg.Servers {
		conn, err := ldap.DialURL(normalizeServerAddr(addr), ldap.DialWithDialer(dialer))
		if err != nil {
			lastErr = err
			continue
		}
		conn.SetTimeout(a.readTimeout())
		return conn, nil
	}
	return nil, fmt.Errorf("could not connect to any of %d configured server(s): %w", len(a.cfg.Servers), lastErr)
}

// normalizeServerAddr defaults a bare hostname (or host:port) to the
// ldap:// scheme - ldap.DialURL requires a full URL and errors with a
// confusing "Unknown scheme ”" otherwise, but a config field literally
// labeled "LDAP servers" invites typing just a hostname (e.g.
// "ldap1.example.com"), which is the common case, not the exception.
// Anything already containing "://" (ldap:// or ldaps://) is left alone.
func normalizeServerAddr(addr string) string {
	addr = strings.TrimSpace(addr)
	if strings.Contains(addr, "://") {
		return addr
	}
	return "ldap://" + addr
}

func (a *Authenticator) connectTimeout() time.Duration {
	if a.cfg.ConnectTimeout > 0 {
		return a.cfg.ConnectTimeout
	}
	return 10 * time.Second
}

func (a *Authenticator) readTimeout() time.Duration {
	if a.cfg.ReadTimeout > 0 {
		return a.cfg.ReadTimeout
	}
	return 10 * time.Second
}

func (a *Authenticator) findUserDN(conn *ldap.Conn, username string) (string, error) {
	req := ldap.NewSearchRequest(
		a.cfg.UserSearchBase,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		buildUserSearchFilter(a.cfg.UserSearchFilter, username),
		[]string{"dn"},
		nil,
	)
	res, err := conn.Search(req)
	if err != nil {
		return "", fmt.Errorf("ldapauth: searching for user %q: %w", username, err)
	}
	if len(res.Entries) == 0 {
		return "", nil
	}
	return res.Entries[0].DN, nil
}

// buildUserSearchFilter substitutes the escaped username into "{0}" in
// filterTemplate (e.g. "sAMAccountName={0}") - a pure function so it's
// testable without a live directory.
func buildUserSearchFilter(filterTemplate, username string) string {
	filterTemplate = ensureParenWrapped(filterTemplate)
	return strings.ReplaceAll(filterTemplate, "{0}", ldap.EscapeFilter(username))
}

// ensureParenWrapped wraps filter in parentheses if it isn't already -
// a bare "sAMAccountName={0}" is a natural, forgiving thing to type into
// a settings field labeled "User search filter" (as opposed to the
// stricter "(sAMAccountName={0})" LDAP actually requires), and go-ldap
// rejects an unwrapped filter outright ("filter does not start with an
// '('") rather than tolerating it.
func ensureParenWrapped(filter string) string {
	filter = strings.TrimSpace(filter)
	if strings.HasPrefix(filter, "(") {
		return filter
	}
	return "(" + filter + ")"
}

func (a *Authenticator) isMemberOfAllowedGroup(conn *ldap.Conn, userDN string) (bool, error) {
	for _, group := range a.cfg.AllowedGroups {
		groupDN, err := a.resolveGroupDN(conn, group)
		if err != nil {
			return false, err
		}
		if groupDN == "" {
			// Configured group doesn't exist in the directory - skip it
			// rather than failing the whole check, so one stale/typo'd
			// entry in AllowedGroups doesn't lock out every user relying
			// on a different, valid entry.
			continue
		}
		req := ldap.NewSearchRequest(
			userDN,
			ldap.ScopeBaseObject, ldap.NeverDerefAliases, 0, 0, false,
			buildNestedGroupFilter(groupDN),
			[]string{"dn"},
			nil,
		)
		res, err := conn.Search(req)
		if err != nil {
			return false, fmt.Errorf("ldapauth: checking membership in %q: %w", group, err)
		}
		if len(res.Entries) > 0 {
			return true, nil
		}
	}
	return false, nil
}

// buildNestedGroupFilter builds the AD filter that matches an entry
// (evaluated with the user's own DN as the search base) whose memberOf
// chain includes groupDN, directly or through nested groups.
func buildNestedGroupFilter(groupDN string) string {
	return fmt.Sprintf("(memberOf:%s:=%s)", nestedGroupMatchingRule, ldap.EscapeFilter(groupDN))
}

// isDN reports whether s already looks like a distinguished name (e.g.
// "CN=admins,OU=Groups,DC=example,DC=com") rather than a bare
// group name (e.g. "admins") that still needs resolving via a
// search.
func isDN(s string) bool {
	return strings.Contains(s, "=")
}

// resolveGroupDN returns group unchanged if it already looks like a DN,
// otherwise searches for it as a bare group name (e.g. "admins")
// under the same user search base. Returns "" (not an error) if no such
// group exists.
func (a *Authenticator) resolveGroupDN(conn *ldap.Conn, group string) (string, error) {
	if isDN(group) {
		return group, nil
	}
	req := ldap.NewSearchRequest(
		a.cfg.UserSearchBase,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		fmt.Sprintf("(&(objectClass=group)(cn=%s))", ldap.EscapeFilter(group)),
		[]string{"dn"},
		nil,
	)
	res, err := conn.Search(req)
	if err != nil {
		return "", fmt.Errorf("ldapauth: resolving group %q: %w", group, err)
	}
	if len(res.Entries) == 0 {
		return "", nil
	}
	return res.Entries[0].DN, nil
}

// DynamicAuthenticator rebuilds its configuration on every Authenticate
// call via configFn, so config changes take effect immediately without a
// process restart - unlike a fixed Authenticator built once at startup.
// This is what lets the admin UI's /settings page store LDAP config in
// the shared database and have every instance pick up an edit right
// away, the same way the shared admin login already works.
type DynamicAuthenticator struct {
	// configFn returns (cfg, enabled, err). enabled=false denies without
	// even attempting a connection - the caller doesn't need to check
	// "is LDAP configured" separately before calling Authenticate.
	configFn func() (Config, bool, error)
}

// NewDynamic wraps configFn as an Authenticator-like value satisfying the
// same Authenticate(username, password) (bool, error) signature as
// *Authenticator - it deliberately does NOT validate configFn's output
// up front (unlike New), since "not configured yet" or "temporarily
// invalid" are both normal, expected states for a database-backed config
// an admin might not have filled in (or might be actively editing) yet.
func NewDynamic(configFn func() (Config, bool, error)) *DynamicAuthenticator {
	return &DynamicAuthenticator{configFn: configFn}
}

func (d *DynamicAuthenticator) Authenticate(username, password string) (bool, error) {
	cfg, enabled, err := d.configFn()
	if err != nil {
		return false, fmt.Errorf("ldapauth: loading dynamic config: %w", err)
	}
	if !enabled {
		return false, nil
	}
	auth, err := New(cfg)
	if err != nil {
		return false, fmt.Errorf("ldapauth: stored config is invalid: %w", err)
	}
	return auth.Authenticate(username, password)
}

// TestConnection checks that cfg's servers/service-account bind work and
// reports whether each of cfg.AllowedGroups resolves to a real AD group -
// meant for validating a configuration from a settings UI BEFORE saving/
// enabling it, e.g. while an admin is still deciding on AllowedGroups.
// Unlike New, this does not require AllowedGroups to be non-empty and
// does not construct a real Authenticator - it never checks an end
// user's own credentials (Authenticate is what does that), only whether
// the service account itself can connect and search.
func TestConnection(cfg Config) (string, error) {
	if len(cfg.Servers) == 0 {
		return "", fmt.Errorf("at least one server is required")
	}
	if cfg.ManagerDN == "" {
		return "", fmt.Errorf("service account DN is required")
	}
	if cfg.ManagerPassword == "" {
		return "", fmt.Errorf("service account password is required")
	}
	if cfg.UserSearchBase == "" {
		return "", fmt.Errorf("user search base is required")
	}

	a := &Authenticator{cfg: cfg}
	conn, err := a.dial()
	if err != nil {
		return "", fmt.Errorf("connecting: %w", err)
	}
	defer conn.Close()

	if err := conn.Bind(cfg.ManagerDN, cfg.ManagerPassword); err != nil {
		return "", fmt.Errorf("service account bind failed: %w", err)
	}

	lines := []string{"connected and bound as the service account successfully"}
	for _, group := range cfg.AllowedGroups {
		dn, err := a.resolveGroupDN(conn, group)
		if err != nil {
			lines = append(lines, fmt.Sprintf("group %q: error resolving (%v)", group, err))
			continue
		}
		if dn == "" {
			lines = append(lines, fmt.Sprintf("group %q: NOT FOUND in the directory", group))
		} else {
			lines = append(lines, fmt.Sprintf("group %q: found (%s)", group, dn))
		}
	}
	return strings.Join(lines, "; "), nil
}
