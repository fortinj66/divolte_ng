// Package adminui is the web UI for viewing/editing the Avro schema and
// mapping rules and hot-reloading them into the running server - new
// functionality versus the original Java server, which only supported
// hand-editing the .avsc/.groovy files on disk with a service restart.
package adminui

import (
	"crypto/rand"
	"crypto/subtle"
	"embed"
	"encoding/hex"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/example/divolte-rewrite/internal/avroenc"
	"github.com/example/divolte-rewrite/internal/event"
	"github.com/example/divolte-rewrite/internal/mapping"
	"github.com/example/divolte-rewrite/internal/store"
)

//go:embed templates/*.html
var templateFS embed.FS

//go:embed static/*.svg static/*.ico
var staticFS embed.FS

// Publisher is satisfied by httpserver.Server - kept as a narrow interface
// here so adminui doesn't need to import httpserver (avoiding an import
// cycle, and keeping this package testable with a fake).
type Publisher interface {
	Publish(mappingCfg *mapping.Config, codec *avroenc.Codec)
}

// LDAPAuthenticator is an optional SECOND way to satisfy Basic Auth,
// alongside the shared username/password stored in the database (see
// basicAuth) - satisfied by internal/ldapauth.Authenticator. Kept as a
// narrow interface, not a direct dependency on internal/ldapauth, so this
// package stays testable with a fake and doesn't need to know anything
// AD-specific.
type LDAPAuthenticator interface {
	Authenticate(username, password string) (bool, error)
}

// LDAPTestFunc validates an LDAP configuration from the /settings page
// BEFORE it's saved/enabled - e.g. so an admin can confirm the service
// account and allowed groups are correct without first committing them.
// Returns a human-readable summary on success; a non-nil error means the
// test itself failed (bad connection, bad bind, etc.), not "no groups
// matched" (which is reported inside the summary string instead).
// Satisfied by a thin wrapper around internal/ldapauth.TestConnection -
// kept as a plain func type rather than growing the LDAPAuthenticator
// interface, since this package still shouldn't need to know
// internal/ldapauth's Config shape.
type LDAPTestFunc func(servers []string, managerDN, managerPassword, userSearchBase, userSearchFilter string, allowedGroups []string) (string, error)

// PublishSyncField is one schema field passed to PublishSync - just the
// name and raw Avro type JSON, enough for downstream syncers (NiFi,
// Druid) to act on without this package needing to import either of
// their concrete config types.
type PublishSyncField struct {
	Name     string
	TypeJSON string
}

// PublishSyncFunc pushes a freshly-published schema out to whatever
// downstream systems are configured (NiFi's parameter context, Druid's
// supervisor spec) - called after a normal Publish already succeeded
// against this instance. Returns a human-readable summary of what
// happened; a non-nil error means at least one configured downstream
// system failed, but the summary string still describes each one
// attempted (so a partial failure - e.g. NiFi succeeded, Druid failed -
// is fully visible, not just the first error). Nil disables this
// entirely (every existing deployment's behavior before this feature).
type PublishSyncFunc func(schemaJSON string, fields []PublishSyncField) (string, error)

// SyncTestFunc validates a NiFi or Druid sync configuration from
// /settings before it's saved/enabled, mirroring LDAPTestFunc's role for
// LDAP config.
type SyncTestFunc func(fieldValues map[string]string) (string, error)

// Config configures the admin UI.
type Config struct {
	Store            *store.Store
	Publisher        Publisher
	SchemaNamespace  string // e.g. "com.example.divolte.record"
	SchemaRecordName string // e.g. "example_event"

	// LDAPAuth, if non-nil, is tried whenever the presented Basic Auth
	// credentials don't match the shared database login - either
	// credential set grants access. Nil (the default) means LDAP auth is
	// simply not offered, matching every existing deployment's behavior
	// until it's explicitly configured.
	LDAPAuth LDAPAuthenticator

	// LDAPTest, if non-nil, powers the /settings page's "Test LDAP
	// connection" button. Nil disables that button's server-side handler
	// (it responds with a clear "not available" message rather than a
	// generic 404/500).
	LDAPTest LDAPTestFunc

	// PublishSync, if non-nil, is called after every successful Publish
	// to push the new schema to NiFi/Druid as configured. See
	// PublishSyncFunc's doc comment.
	PublishSync PublishSyncFunc

	// NiFiTest/DruidTest power the /settings page's "Test" buttons for
	// the NiFi and Druid sync sections, mirroring LDAPTest's role.
	NiFiTest  SyncTestFunc
	DruidTest SyncTestFunc

	// KafkaReconcile, if non-nil, is called after every /kafka-targets
	// create/update/delete so a saved change takes effect on the very
	// next event, without a restart - unlike PublishSync, this is NOT
	// tied to the schema/mapping Publish flow, since Kafka output
	// targets are their own independent concern.
	KafkaReconcile func() error

	// KafkaTest powers the /kafka-targets form's "Test connection"
	// button, mirroring NiFiTest/DruidTest's role.
	KafkaTest SyncTestFunc

	// URIPrefix lets this admin UI be reachable under a path prefix (e.g.
	// "/admin") behind a reverse proxy that strips the prefix before
	// forwarding - so gin's own route registration below never needs to
	// know about it (every route stays registered at "/", "/fields/new",
	// etc., matching exactly what the proxy forwards). The only place this
	// matters is generating URLs that go back out to the browser -
	// redirect Location headers and the links/actions/fetch() URL baked
	// into the rendered HTML - since those are root-relative and a
	// browser resolves a leading "/" against the site root, not the
	// current directory; without the prefix on those, every link on the
	// page would silently escape back out to the site root once clicked.
	// Empty (the default, and what every test uses) means root-mounted,
	// matching the original behavior - must not have a trailing slash.
	URIPrefix string
}

// url prepends URIPrefix to a root-relative path, for anything that ends
// up in front of the browser (redirect targets, template links) - see the
// URIPrefix doc comment above for why this is needed at all.
func (h *handlers) url(path string) string {
	return h.cfg.URIPrefix + path
}

// New builds the admin UI's http.Handler, gated behind HTTP basic auth.
func New(cfg Config) (http.Handler, error) {
	tmpl, err := template.ParseFS(templateFS, "templates/*.html")
	if err != nil {
		return nil, fmt.Errorf("adminui: parsing templates: %w", err)
	}

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())
	r.SetHTMLTemplate(tmpl)

	h := &handlers{cfg: cfg}

	// /logout is intentionally registered OUTSIDE the auth group below -
	// it always returns 401, regardless of whether valid credentials are
	// presented, which is what makes a browser actually drop its cached
	// Basic Auth credential for this origin (Basic Auth has no real
	// logout of its own; this is the standard workaround). Bypasses
	// primaryRedirect too - logging out is cheap and safe to handle
	// locally on whichever instance the browser happened to be on.
	r.GET("/logout", func(c *gin.Context) {
		c.Header("WWW-Authenticate", `Basic realm="`+basicAuthRealm+`"`)
		c.String(http.StatusUnauthorized, "Logged out - close this tab or browser window to fully clear the cached credentials.")
	})

	// /schema is intentionally public (no Basic Auth) - it's read-only
	// metadata, not a sensitive control surface, and the whole point is
	// letting OTHER processes (a future JSON-publishing plugin, some
	// other team's tooling) discover the current schema without needing
	// admin credentials or direct database access. Always reflects what
	// was last actually PUBLISHED, not any in-progress unpublished edit.
	r.GET("/schema", func(c *gin.Context) {
		schemaJSON, err := cfg.Store.GetPublishedSchemaJSON(cfg.SchemaNamespace, cfg.SchemaRecordName)
		if err != nil {
			c.String(http.StatusInternalServerError, "loading published schema: %v", err)
			return
		}
		c.Data(http.StatusOK, "application/json", []byte(schemaJSON))
	})

	// primaryRedirect must run BEFORE basicAuth: otherwise a non-primary
	// instance would challenge the browser for a login it's about to
	// redirect away from anyway - a real usability problem when every
	// instance shares one login (the browser would show a confusing
	// Basic Auth prompt for the wrong host before ever reaching the
	// primary). Running it first means only the actual primary instance
	// ever prompts for credentials.
	auth := r.Group("/", h.primaryRedirect, h.basicAuth, csrfProtect)
	auth.GET("/static/logo.svg", func(c *gin.Context) {
		c.FileFromFS("static/logo.svg", http.FS(staticFS))
	})
	auth.GET("/static/favicon.ico", func(c *gin.Context) {
		c.FileFromFS("static/favicon.ico", http.FS(staticFS))
	})
	auth.GET("/settings", h.settingsForm)
	auth.POST("/settings", h.settingsUpdate)
	auth.POST("/settings/test-ldap", h.settingsTestLDAP)

	auth.GET("/nifi-targets", h.nifiTargetsList)
	auth.GET("/nifi-targets/new", h.nifiTargetNewForm)
	auth.POST("/nifi-targets", h.nifiTargetCreate)
	auth.GET("/nifi-targets/:id/edit", h.nifiTargetEditForm)
	auth.POST("/nifi-targets/:id", h.nifiTargetUpdate)
	auth.POST("/nifi-targets/:id/delete", h.nifiTargetDelete)
	auth.POST("/nifi-targets/:id/test", h.nifiTargetTest)

	auth.GET("/druid-targets", h.druidTargetsList)
	auth.GET("/druid-targets/new", h.druidTargetNewForm)
	auth.POST("/druid-targets", h.druidTargetCreate)
	auth.GET("/druid-targets/:id/edit", h.druidTargetEditForm)
	auth.POST("/druid-targets/:id", h.druidTargetUpdate)
	auth.POST("/druid-targets/:id/delete", h.druidTargetDelete)
	auth.POST("/druid-targets/:id/test", h.druidTargetTest)

	auth.GET("/kafka-targets", h.kafkaTargetsList)
	auth.GET("/kafka-targets/new", h.kafkaTargetNewForm)
	auth.POST("/kafka-targets", h.kafkaTargetCreate)
	auth.GET("/kafka-targets/:id/edit", h.kafkaTargetEditForm)
	auth.POST("/kafka-targets/:id", h.kafkaTargetUpdate)
	auth.POST("/kafka-targets/:id/delete", h.kafkaTargetDelete)
	auth.POST("/kafka-targets/:id/test", h.kafkaTargetTest)

	auth.GET("/", h.list)
	auth.GET("/fields/new", h.newForm)
	auth.POST("/fields", h.create)
	auth.GET("/fields/:name/edit", h.editForm)
	auth.POST("/fields/:name", h.update)
	auth.POST("/fields/:name/delete", h.delete)
	auth.POST("/fields/reorder", h.reorder)
	auth.POST("/fields/set-order", h.setOrder)
	auth.POST("/publish", h.publish)
	auth.POST("/revert", h.revert)

	return r, nil
}

const (
	csrfCookieName = "csrf_token"
	csrfFormField  = "csrf_token"
	csrfHeaderName = "X-CSRF-Token"
	csrfContextKey = "csrfToken"
)

// csrfProtect is a synchronizer-token CSRF defense: every response (GET or
// POST) carries a random token in an HttpOnly cookie, and every rendered
// form embeds that same value as a hidden field (via csrfToken(c) - see
// list/newForm/editForm below); the set-order fetch() call in list.html
// sends it as the X-CSRF-Token header instead, since it isn't a form POST.
// Every state-mutating POST here must present the same value back, either
// way. Basic Auth alone doesn't defend against this: browsers re-attach
// cached Basic Auth credentials to any request to this origin regardless
// of which page initiated it (unlike a cookie, Basic Auth has no SameSite
// concept), so without this, a page an admin merely has open in another
// tab could silently submit a hidden auto-submitting form to POST
// /publish or /fields/:name/delete and it would succeed. The attacker page
// cannot read this HttpOnly cookie's value, so it cannot reproduce it in
// the hidden field/header it forges - the browser attaches the cookie
// automatically, but only this server's own previously-rendered page ever
// had the value to put in the accompanying field.
func csrfProtect(c *gin.Context) {
	token, err := c.Cookie(csrfCookieName)
	if err != nil || token == "" {
		token = randomCSRFToken()
		c.SetCookie(csrfCookieName, token, 0, "/", "", false, true)
	}
	c.Set(csrfContextKey, token)

	if c.Request.Method == http.MethodPost {
		presented := c.PostForm(csrfFormField)
		if presented == "" {
			presented = c.GetHeader(csrfHeaderName)
		}
		if presented == "" || presented != token {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}
	}
	c.Next()
}

func randomCSRFToken() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		// crypto/rand.Read failing means the OS entropy source is broken -
		// nothing sane to do but panic; refusing every request until the
		// process restarts is safer than silently issuing a weak/predictable
		// token.
		panic(fmt.Sprintf("adminui: reading random bytes for CSRF token: %v", err))
	}
	return hex.EncodeToString(b)
}

// csrfToken returns the current request's CSRF token (set by csrfProtect)
// for embedding as a hidden field in a rendered form.
func csrfToken(c *gin.Context) string {
	v, _ := c.Get(csrfContextKey)
	s, _ := v.(string)
	return s
}

type handlers struct {
	cfg Config
}

const basicAuthRealm = "Authorization Required"

// basicAuth gates every admin-UI request behind HTTP Basic Auth. The
// presented credentials can satisfy EITHER of two independent checks:
// the shared login stored in the database (internal/store's
// AdminSettings, checked on every request rather than a static in-memory
// credential - this is what lets the login be changed at runtime via
// /settings and be the SAME login across every Divolte instance), OR (if
// LDAPAuth is configured) a valid Active Directory login for a user in
// an allowed group. Either one is sufficient; there's no "mode" to pick.
func (h *handlers) basicAuth(c *gin.Context) {
	user, pass, hasAuth := c.Request.BasicAuth()
	if hasAuth {
		ok, err := h.checkCredentials(user, pass)
		if err != nil {
			c.String(http.StatusInternalServerError, "checking credentials: %v", err)
			c.Abort()
			return
		}
		if ok {
			c.Set(gin.AuthUserKey, user)
			return
		}
	}
	c.Header("WWW-Authenticate", `Basic realm="`+basicAuthRealm+`"`)
	c.AbortWithStatus(http.StatusUnauthorized)
}

// checkCredentials reports whether user/pass match the shared database
// login, using constant-time comparison so a login attempt can't be used
// to time-probe the stored credential character-by-character. Only
// consults LDAPAuth if the database check didn't already succeed. An LDAP
// connection/search failure is logged and treated as a denial (fails
// closed) rather than surfaced as a 500 to the caller, matching how a
// wrong password behaves - a directory outage shouldn't distinguish
// "which auth method is misconfigured" to an unauthenticated caller.
func (h *handlers) checkCredentials(user, pass string) (bool, error) {
	settings, err := h.cfg.Store.GetAdminSettings()
	if err != nil {
		return false, fmt.Errorf("loading admin settings: %w", err)
	}
	if constantTimeEqual(user, settings.Username) && constantTimeEqual(pass, settings.Password) {
		return true, nil
	}
	if h.cfg.LDAPAuth == nil {
		return false, nil
	}
	ok, err := h.cfg.LDAPAuth.Authenticate(user, pass)
	if err != nil {
		log.Printf("adminui: LDAP authentication error for user %q: %v", user, err)
		return false, nil
	}
	return ok, nil
}

func constantTimeEqual(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// primaryRedirect sends a browser hitting any instance's admin UI to the
// designated primary instance's admin URL (AdminSettings.PrimaryURL),
// e.g. so hitting a non-primary instance's /admin/ lands on the primary's
// instead, once one is set via /settings. Every instance runs this same
// check against the same shared database row, so they all agree on where
// "the" admin UI actually is - visiting the primary itself is a no-op
// (matched by host), not a redirect loop.
func (h *handlers) primaryRedirect(c *gin.Context) {
	// Always reachable directly, on every instance, regardless of the
	// stored primary - otherwise a wrong/unreachable primary URL would
	// strand every instance (including the real primary) with no way to
	// fix it back through the UI.
	if c.Request.URL.Path == "/settings" {
		return
	}
	settings, err := h.cfg.Store.GetAdminSettings()
	if err != nil {
		c.String(http.StatusInternalServerError, "loading admin settings: %v", err)
		c.Abort()
		return
	}
	if settings.PrimaryURL == "" {
		return
	}
	target, err := url.Parse(settings.PrimaryURL)
	if err != nil || target.Host == "" {
		return
	}
	if strings.EqualFold(c.Request.Host, target.Host) {
		return
	}
	dest := strings.TrimRight(settings.PrimaryURL, "/") + c.Request.URL.Path
	if rq := c.Request.URL.RawQuery; rq != "" {
		dest += "?" + rq
	}
	c.Redirect(http.StatusFound, dest)
	c.Abort()
}

func (h *handlers) settingsForm(c *gin.Context) {
	settings, err := h.cfg.Store.GetAdminSettings()
	if err != nil {
		c.String(http.StatusInternalServerError, "loading admin settings: %v", err)
		return
	}
	ldapSettings, err := h.cfg.Store.GetLDAPSettings()
	if err != nil {
		c.String(http.StatusInternalServerError, "loading ldap settings: %v", err)
		return
	}
	c.HTML(http.StatusOK, "settings.html", gin.H{
		"Username":   settings.Username,
		"PrimaryURL": settings.PrimaryURL,

		"LDAPEnabled":          ldapSettings.Enabled,
		"LDAPServers":          strings.Join(ldapSettings.Servers, "\n"),
		"LDAPManagerDN":        ldapSettings.ManagerDN,
		"LDAPUserSearchBase":   ldapSettings.UserSearchBase,
		"LDAPUserSearchFilter": ldapSettings.UserSearchFilter,
		"LDAPAllowedGroups":    strings.Join(ldapSettings.AllowedGroups, "\n"),

		"Flash":      c.Query("flash"),
		"FlashClass": c.DefaultQuery("flash_class", "ok"),
		"CSRFToken":  csrfToken(c),
		"URIPrefix":  h.cfg.URIPrefix,
	})
}

// settingsUpdate saves the shared admin login and/or the primary admin
// URL. A blank password field leaves the current password unchanged
// (list.html-style forms don't round-trip the current password back into
// the field, so blank must mean "no change" rather than "set to empty").
func (h *handlers) settingsUpdate(c *gin.Context) {
	username := strings.TrimSpace(c.PostForm("username"))
	password := c.PostForm("password")
	primaryURL := strings.TrimSpace(c.PostForm("primary_url"))

	if username == "" {
		c.Redirect(http.StatusSeeOther, h.url("/settings?flash_class=err&flash="+template.URLQueryEscaper("username is required")))
		return
	}
	current, err := h.cfg.Store.GetAdminSettings()
	if err != nil {
		c.String(http.StatusInternalServerError, "loading admin settings: %v", err)
		return
	}
	if password == "" {
		password = current.Password
	}
	if err := h.cfg.Store.SetAdminCredentials(username, password); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/settings?flash_class=err&flash="+template.URLQueryEscaper(err.Error())))
		return
	}
	if err := h.cfg.Store.SetPrimaryURL(primaryURL); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/settings?flash_class=err&flash="+template.URLQueryEscaper(err.Error())))
		return
	}

	ldapEnabled := c.PostForm("ldap_enabled") != ""
	ldapServers := splitFormLines(c.PostForm("ldap_servers"))
	ldapManagerDN := strings.TrimSpace(c.PostForm("ldap_manager_dn"))
	ldapManagerPassword := c.PostForm("ldap_manager_password")
	ldapUserSearchBase := strings.TrimSpace(c.PostForm("ldap_user_search_base"))
	ldapUserSearchFilter := strings.TrimSpace(c.PostForm("ldap_user_search_filter"))
	ldapAllowedGroups := splitFormLines(c.PostForm("ldap_allowed_groups"))

	if ldapEnabled && len(ldapAllowedGroups) == 0 {
		c.Redirect(http.StatusSeeOther, h.url("/settings?flash_class=err&flash="+template.URLQueryEscaper("LDAP requires at least one allowed group - this UI can edit live schema/mapping, so it never authenticates without a group restriction")))
		return
	}
	currentLDAP, err := h.cfg.Store.GetLDAPSettings()
	if err != nil {
		c.String(http.StatusInternalServerError, "loading ldap settings: %v", err)
		return
	}
	if ldapManagerPassword == "" {
		ldapManagerPassword = currentLDAP.ManagerPassword
	}
	if err := h.cfg.Store.SetLDAPSettings(store.LDAPSettings{
		Enabled:          ldapEnabled,
		Servers:          ldapServers,
		ManagerDN:        ldapManagerDN,
		ManagerPassword:  ldapManagerPassword,
		UserSearchBase:   ldapUserSearchBase,
		UserSearchFilter: ldapUserSearchFilter,
		AllowedGroups:    ldapAllowedGroups,
	}); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/settings?flash_class=err&flash="+template.URLQueryEscaper(err.Error())))
		return
	}

	c.Redirect(http.StatusSeeOther, h.url("/settings?flash="+template.URLQueryEscaper("settings saved")))
}

// splitFormLines splits a <textarea>'s posted value into trimmed,
// non-empty lines - tolerates \r\n (browser textareas normalize to \n,
// but don't rely on that unconditionally).
func splitFormLines(s string) []string {
	var out []string
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if line != "" {
			out = append(out, line)
		}
	}
	return out
}

// settingsTestLDAP validates the posted LDAP fields (which may not have
// been saved yet - this reads directly from the form, not the database)
// via h.cfg.LDAPTest, and responds with a small JSON body rather than a
// redirect, so the settings page's Test button can show the result
// inline without a full page reload. If ldap_manager_password was left
// blank (the settings form's "keep the current password" convention),
// falls back to whatever's already stored, so testing an otherwise-
// unchanged config doesn't require re-typing the password.
func (h *handlers) settingsTestLDAP(c *gin.Context) {
	if h.cfg.LDAPTest == nil {
		c.JSON(http.StatusOK, gin.H{"ok": false, "message": "LDAP testing is not available in this build"})
		return
	}

	managerPassword := c.PostForm("ldap_manager_password")
	if managerPassword == "" {
		if current, err := h.cfg.Store.GetLDAPSettings(); err == nil {
			managerPassword = current.ManagerPassword
		}
	}

	message, err := h.cfg.LDAPTest(
		splitFormLines(c.PostForm("ldap_servers")),
		strings.TrimSpace(c.PostForm("ldap_manager_dn")),
		managerPassword,
		strings.TrimSpace(c.PostForm("ldap_user_search_base")),
		strings.TrimSpace(c.PostForm("ldap_user_search_filter")),
		splitFormLines(c.PostForm("ldap_allowed_groups")),
	)
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"ok": false, "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "message": message})
}

// --- NiFi targets (one row per NiFi cluster to push the Avro schema to,
// e.g. nifi-02 and nifi-01 are separate clusters) ---

func (h *handlers) nifiTargetsList(c *gin.Context) {
	targets, err := h.cfg.Store.ListNiFiAvroTargets()
	if err != nil {
		c.String(http.StatusInternalServerError, "loading nifi targets: %v", err)
		return
	}
	c.HTML(http.StatusOK, "nifi_targets.html", gin.H{
		"Targets": targets, "Flash": c.Query("flash"), "FlashClass": c.DefaultQuery("flash_class", "ok"),
		"CSRFToken": csrfToken(c), "URIPrefix": h.cfg.URIPrefix,
	})
}

func (h *handlers) nifiTargetNewForm(c *gin.Context) {
	c.HTML(http.StatusOK, "nifi_target_form.html", gin.H{
		"IsNew": true, "Target": store.NiFiAvroTarget{}, "CSRFToken": csrfToken(c), "URIPrefix": h.cfg.URIPrefix,
	})
}

func (h *handlers) nifiTargetEditForm(c *gin.Context) {
	id := c.Param("id")
	target, err := h.cfg.Store.GetNiFiAvroTarget(id)
	if err != nil {
		c.String(http.StatusInternalServerError, "loading nifi target: %v", err)
		return
	}
	if target == nil {
		c.String(http.StatusNotFound, "no such nifi target: %s", id)
		return
	}
	c.HTML(http.StatusOK, "nifi_target_form.html", gin.H{
		"IsNew": false, "Target": *target, "CSRFToken": csrfToken(c), "URIPrefix": h.cfg.URIPrefix,
	})
}

// nifiTargetFromForm reads the posted target fields. current, if non-nil,
// supplies the fallback client key when the form's key field was left
// blank (the "don't change the secret" convention used elsewhere).
func nifiTargetFromForm(c *gin.Context, id string, current *store.NiFiAvroTarget) store.NiFiAvroTarget {
	clientKey := c.PostForm("client_key")
	if clientKey == "" && current != nil {
		clientKey = current.ClientKeyPEM
	}
	clientKeyPassphrase := c.PostForm("client_key_passphrase")
	if clientKeyPassphrase == "" && current != nil {
		clientKeyPassphrase = current.ClientKeyPassphrase
	}
	return store.NiFiAvroTarget{
		ID:                  id,
		Enabled:             c.PostForm("enabled") != "",
		BaseURL:             strings.TrimSpace(c.PostForm("base_url")),
		ClientCertPEM:       c.PostForm("client_cert"),
		ClientKeyPEM:        clientKey,
		ClientKeyPassphrase: clientKeyPassphrase,
		CACertPEM:           c.PostForm("ca_cert"),
		ParameterContextID:  strings.TrimSpace(c.PostForm("parameter_context_id")),
		ParameterName:       strings.TrimSpace(c.PostForm("parameter_name")),
		ControllerServiceID: strings.TrimSpace(c.PostForm("controller_service_id")),
	}
}

func (h *handlers) nifiTargetCreate(c *gin.Context) {
	id := strings.TrimSpace(c.PostForm("id"))
	if id == "" {
		c.Redirect(http.StatusSeeOther, h.url("/nifi-targets/new?flash_class=err&flash="+template.URLQueryEscaper("name is required")))
		return
	}
	if err := h.cfg.Store.UpsertNiFiAvroTarget(nifiTargetFromForm(c, id, nil)); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/nifi-targets/new?flash_class=err&flash="+template.URLQueryEscaper(err.Error())))
		return
	}
	c.Redirect(http.StatusSeeOther, h.url("/nifi-targets?flash="+template.URLQueryEscaper("target added: "+id)))
}

func (h *handlers) nifiTargetUpdate(c *gin.Context) {
	id := c.Param("id")
	current, err := h.cfg.Store.GetNiFiAvroTarget(id)
	if err != nil {
		c.String(http.StatusInternalServerError, "loading nifi target: %v", err)
		return
	}
	if err := h.cfg.Store.UpsertNiFiAvroTarget(nifiTargetFromForm(c, id, current)); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/nifi-targets/"+id+"/edit?flash_class=err&flash="+template.URLQueryEscaper(err.Error())))
		return
	}
	c.Redirect(http.StatusSeeOther, h.url("/nifi-targets?flash="+template.URLQueryEscaper("target saved: "+id)))
}

func (h *handlers) nifiTargetDelete(c *gin.Context) {
	id := c.Param("id")
	if err := h.cfg.Store.DeleteNiFiAvroTarget(id); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/nifi-targets?flash_class=err&flash="+template.URLQueryEscaper(err.Error())))
		return
	}
	c.Redirect(http.StatusSeeOther, h.url("/nifi-targets?flash="+template.URLQueryEscaper("target deleted: "+id)))
}

// nifiTargetTest validates the posted fields (not necessarily saved yet)
// via h.cfg.NiFiTest, responding with inline JSON so the form's Test
// button can show the result without a page reload. A blank client-key
// field falls back to the target's currently stored key (by :id), so
// testing an otherwise-unchanged config doesn't require re-pasting the
// private key.
func (h *handlers) nifiTargetTest(c *gin.Context) {
	if h.cfg.NiFiTest == nil {
		c.JSON(http.StatusOK, gin.H{"ok": false, "message": "NiFi testing is not available in this build"})
		return
	}
	clientKey := c.PostForm("client_key")
	clientKeyPassphrase := c.PostForm("client_key_passphrase")
	if clientKey == "" || clientKeyPassphrase == "" {
		if current, err := h.cfg.Store.GetNiFiAvroTarget(c.Param("id")); err == nil && current != nil {
			if clientKey == "" {
				clientKey = current.ClientKeyPEM
			}
			if clientKeyPassphrase == "" {
				clientKeyPassphrase = current.ClientKeyPassphrase
			}
		}
	}
	message, err := h.cfg.NiFiTest(map[string]string{
		"base_url":              strings.TrimSpace(c.PostForm("base_url")),
		"client_cert":           c.PostForm("client_cert"),
		"client_key":            clientKey,
		"client_key_passphrase": clientKeyPassphrase,
		"ca_cert":               c.PostForm("ca_cert"),
		"parameter_context_id":  strings.TrimSpace(c.PostForm("parameter_context_id")),
		"parameter_name":        strings.TrimSpace(c.PostForm("parameter_name")),
		"controller_service_id": strings.TrimSpace(c.PostForm("controller_service_id")),
	})
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"ok": false, "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "message": message})
}

// --- Druid targets (one row per Druid cluster/supervisor, e.g. dev and
// multiple prod clusters) ---

func (h *handlers) druidTargetsList(c *gin.Context) {
	targets, err := h.cfg.Store.ListDruidTargets()
	if err != nil {
		c.String(http.StatusInternalServerError, "loading druid targets: %v", err)
		return
	}
	c.HTML(http.StatusOK, "druid_targets.html", gin.H{
		"Targets": targets, "Flash": c.Query("flash"), "FlashClass": c.DefaultQuery("flash_class", "ok"),
		"CSRFToken": csrfToken(c), "URIPrefix": h.cfg.URIPrefix,
	})
}

func (h *handlers) druidTargetNewForm(c *gin.Context) {
	c.HTML(http.StatusOK, "druid_target_form.html", gin.H{
		"IsNew": true, "Target": store.DruidTarget{}, "CSRFToken": csrfToken(c), "URIPrefix": h.cfg.URIPrefix,
	})
}

func (h *handlers) druidTargetEditForm(c *gin.Context) {
	id := c.Param("id")
	target, err := h.cfg.Store.GetDruidTarget(id)
	if err != nil {
		c.String(http.StatusInternalServerError, "loading druid target: %v", err)
		return
	}
	if target == nil {
		c.String(http.StatusNotFound, "no such druid target: %s", id)
		return
	}
	c.HTML(http.StatusOK, "druid_target_form.html", gin.H{
		"IsNew": false, "Target": *target, "CSRFToken": csrfToken(c), "URIPrefix": h.cfg.URIPrefix,
	})
}

func druidTargetFromForm(c *gin.Context, id string) store.DruidTarget {
	return store.DruidTarget{
		ID:             id,
		Enabled:        c.PostForm("enabled") != "",
		BaseURL:        strings.TrimSpace(c.PostForm("base_url")),
		SupervisorName: strings.TrimSpace(c.PostForm("supervisor_name")),
	}
}

func (h *handlers) druidTargetCreate(c *gin.Context) {
	id := strings.TrimSpace(c.PostForm("id"))
	if id == "" {
		c.Redirect(http.StatusSeeOther, h.url("/druid-targets/new?flash_class=err&flash="+template.URLQueryEscaper("name is required")))
		return
	}
	if err := h.cfg.Store.UpsertDruidTarget(druidTargetFromForm(c, id)); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/druid-targets/new?flash_class=err&flash="+template.URLQueryEscaper(err.Error())))
		return
	}
	c.Redirect(http.StatusSeeOther, h.url("/druid-targets?flash="+template.URLQueryEscaper("target added: "+id)))
}

func (h *handlers) druidTargetUpdate(c *gin.Context) {
	id := c.Param("id")
	if err := h.cfg.Store.UpsertDruidTarget(druidTargetFromForm(c, id)); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/druid-targets/"+id+"/edit?flash_class=err&flash="+template.URLQueryEscaper(err.Error())))
		return
	}
	c.Redirect(http.StatusSeeOther, h.url("/druid-targets?flash="+template.URLQueryEscaper("target saved: "+id)))
}

func (h *handlers) druidTargetDelete(c *gin.Context) {
	id := c.Param("id")
	if err := h.cfg.Store.DeleteDruidTarget(id); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/druid-targets?flash_class=err&flash="+template.URLQueryEscaper(err.Error())))
		return
	}
	c.Redirect(http.StatusSeeOther, h.url("/druid-targets?flash="+template.URLQueryEscaper("target deleted: "+id)))
}

func (h *handlers) druidTargetTest(c *gin.Context) {
	if h.cfg.DruidTest == nil {
		c.JSON(http.StatusOK, gin.H{"ok": false, "message": "Druid testing is not available in this build"})
		return
	}
	message, err := h.cfg.DruidTest(map[string]string{
		"base_url":        strings.TrimSpace(c.PostForm("base_url")),
		"supervisor_name": strings.TrimSpace(c.PostForm("supervisor_name")),
	})
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"ok": false, "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "message": message})
}

func (h *handlers) kafkaTargetsList(c *gin.Context) {
	targets, err := h.cfg.Store.ListKafkaOutputTargets()
	if err != nil {
		c.String(http.StatusInternalServerError, "loading kafka output targets: %v", err)
		return
	}
	c.HTML(http.StatusOK, "kafka_targets.html", gin.H{
		"Targets": targets, "Flash": c.Query("flash"), "FlashClass": c.DefaultQuery("flash_class", "ok"),
		"CSRFToken": csrfToken(c), "URIPrefix": h.cfg.URIPrefix,
	})
}

func (h *handlers) kafkaTargetNewForm(c *gin.Context) {
	c.HTML(http.StatusOK, "kafka_target_form.html", gin.H{
		"IsNew": true, "Target": store.KafkaOutputTarget{Format: "avro"}, "CSRFToken": csrfToken(c), "URIPrefix": h.cfg.URIPrefix,
	})
}

func (h *handlers) kafkaTargetEditForm(c *gin.Context) {
	id := c.Param("id")
	target, err := h.cfg.Store.GetKafkaOutputTarget(id)
	if err != nil {
		c.String(http.StatusInternalServerError, "loading kafka output target: %v", err)
		return
	}
	if target == nil {
		c.String(http.StatusNotFound, "no such kafka output target: %s", id)
		return
	}
	c.HTML(http.StatusOK, "kafka_target_form.html", gin.H{
		"IsNew": false, "Target": *target, "CSRFToken": csrfToken(c), "URIPrefix": h.cfg.URIPrefix,
	})
}

func kafkaTargetFromForm(c *gin.Context, id string) store.KafkaOutputTarget {
	format := strings.TrimSpace(c.PostForm("format"))
	if format != "json" {
		format = "avro"
	}
	return store.KafkaOutputTarget{
		ID:      id,
		Enabled: c.PostForm("enabled") != "",
		Format:  format,
		Topic:   strings.TrimSpace(c.PostForm("topic")),
		Brokers: strings.TrimSpace(c.PostForm("brokers")),
	}
}

// reconcileKafka calls KafkaReconcile (if configured) so a saved target
// change takes effect on the very next event - a nil KafkaReconcile means
// this build doesn't wire Kafka output at all (e.g. a test build), not an
// error condition.
func (h *handlers) reconcileKafka() error {
	if h.cfg.KafkaReconcile == nil {
		return nil
	}
	return h.cfg.KafkaReconcile()
}

func (h *handlers) kafkaTargetCreate(c *gin.Context) {
	id := strings.TrimSpace(c.PostForm("id"))
	if id == "" {
		c.Redirect(http.StatusSeeOther, h.url("/kafka-targets/new?flash_class=err&flash="+template.URLQueryEscaper("name is required")))
		return
	}
	if err := h.cfg.Store.UpsertKafkaOutputTarget(kafkaTargetFromForm(c, id)); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/kafka-targets/new?flash_class=err&flash="+template.URLQueryEscaper(err.Error())))
		return
	}
	if err := h.reconcileKafka(); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/kafka-targets?flash_class=err&flash="+template.URLQueryEscaper("target saved but failed to connect: "+err.Error())))
		return
	}
	c.Redirect(http.StatusSeeOther, h.url("/kafka-targets?flash="+template.URLQueryEscaper("target added: "+id)))
}

func (h *handlers) kafkaTargetUpdate(c *gin.Context) {
	id := c.Param("id")
	if err := h.cfg.Store.UpsertKafkaOutputTarget(kafkaTargetFromForm(c, id)); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/kafka-targets/"+id+"/edit?flash_class=err&flash="+template.URLQueryEscaper(err.Error())))
		return
	}
	if err := h.reconcileKafka(); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/kafka-targets?flash_class=err&flash="+template.URLQueryEscaper("target saved but failed to connect: "+err.Error())))
		return
	}
	c.Redirect(http.StatusSeeOther, h.url("/kafka-targets?flash="+template.URLQueryEscaper("target saved: "+id)))
}

func (h *handlers) kafkaTargetDelete(c *gin.Context) {
	id := c.Param("id")
	if err := h.cfg.Store.DeleteKafkaOutputTarget(id); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/kafka-targets?flash_class=err&flash="+template.URLQueryEscaper(err.Error())))
		return
	}
	if err := h.reconcileKafka(); err != nil {
		log.Printf("adminui: reconciling kafka targets after delete of %q: %v", id, err)
	}
	c.Redirect(http.StatusSeeOther, h.url("/kafka-targets?flash="+template.URLQueryEscaper("target deleted: "+id)))
}

func (h *handlers) kafkaTargetTest(c *gin.Context) {
	if h.cfg.KafkaTest == nil {
		c.JSON(http.StatusOK, gin.H{"ok": false, "message": "Kafka testing is not available in this build"})
		return
	}
	message, err := h.cfg.KafkaTest(map[string]string{
		"topic":   strings.TrimSpace(c.PostForm("topic")),
		"brokers": strings.TrimSpace(c.PostForm("brokers")),
	})
	if err != nil {
		c.JSON(http.StatusOK, gin.H{"ok": false, "message": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "message": message})
}

type fieldRow struct {
	Position    int
	Name        string
	TypeJSON    string
	HasDefault  bool
	DefaultJSON string
	RuleSummary string
}

func summarizeRule(r *store.MappingRule) string {
	if r == nil {
		return "(none - field will always be absent)"
	}
	switch {
	case r.Builtin != "":
		return "builtin: " + r.Builtin
	case r.EventParam != "":
		s := "event_param: " + r.EventParam
		if r.Coerce != "" {
			s += " (" + r.Coerce + ")"
		}
		return s
	case r.EventParamPath != "":
		s := "event_param_path: " + r.EventParamPath
		if r.Coerce != "" {
			s += " (" + r.Coerce + ")"
		}
		return s
	default:
		return "(none - field will always be absent)"
	}
}

func (h *handlers) list(c *gin.Context) {
	fields, err := h.cfg.Store.ListSchemaFields()
	if err != nil {
		c.String(http.StatusInternalServerError, "listing fields: %v", err)
		return
	}
	rules, err := h.cfg.Store.ListMappingRules()
	if err != nil {
		c.String(http.StatusInternalServerError, "listing mapping rules: %v", err)
		return
	}
	ruleByField := make(map[string]store.MappingRule, len(rules))
	for _, r := range rules {
		ruleByField[r.Field] = r
	}

	rows := make([]fieldRow, 0, len(fields))
	for _, f := range fields {
		var rulePtr *store.MappingRule
		if r, ok := ruleByField[f.Name]; ok {
			rulePtr = &r
		}
		rows = append(rows, fieldRow{
			Position: f.Position, Name: f.Name, TypeJSON: f.TypeJSON,
			HasDefault: f.HasDefault, DefaultJSON: f.DefaultJSON,
			RuleSummary: summarizeRule(rulePtr),
		})
	}

	hasChanges, err := h.cfg.Store.HasUnpublishedChanges()
	if err != nil {
		c.String(http.StatusInternalServerError, "checking for unpublished changes: %v", err)
		return
	}

	c.HTML(http.StatusOK, "list.html", gin.H{
		"Fields":                rows,
		"Flash":                 c.Query("flash"),
		"FlashClass":            c.DefaultQuery("flash_class", "ok"),
		"HasUnpublishedChanges": hasChanges,
		"CSRFToken":             csrfToken(c),
		"URIPrefix":             h.cfg.URIPrefix,
	})
}

func (h *handlers) newForm(c *gin.Context) {
	h.renderForm(c, http.StatusOK, gin.H{
		"IsNew": true, "Field": store.SchemaField{}, "Rule": store.MappingRule{}, "SourceKind": "builtin",
		"FT":        friendlyType{BaseType: "string", ItemType: "string", DefaultMode: "none"},
		"CSRFToken": csrfToken(c),
	})
}

func (h *handlers) editForm(c *gin.Context) {
	name := c.Param("name")
	field, rule, err := h.cfg.Store.GetField(name)
	if err != nil {
		c.String(http.StatusInternalServerError, "loading field: %v", err)
		return
	}
	if field == nil {
		c.String(http.StatusNotFound, "no such field: %s", name)
		return
	}
	if rule == nil {
		rule = &store.MappingRule{Field: name}
	}
	h.renderForm(c, http.StatusOK, gin.H{
		"IsNew": false, "Field": *field, "Rule": *rule, "SourceKind": sourceKindOf(*rule),
		"FT": parseFriendlyType(field.TypeJSON, field.HasDefault, field.DefaultJSON),
	})
}

// renderForm fills in the type-option lists (constant across every render)
// and the CSRF token.
func (h *handlers) renderForm(c *gin.Context, status int, data gin.H) {
	data["BaseTypes"] = primitiveTypes
	data["ItemTypes"] = arrayItemTypes
	data["BuiltinGroups"] = groupedBuiltinOptions()
	data["CSRFToken"] = csrfToken(c)
	data["URIPrefix"] = h.cfg.URIPrefix
	c.HTML(status, "form.html", data)
}

func sourceKindOf(r store.MappingRule) string {
	switch {
	case r.EventParam != "":
		return "event_param"
	case r.EventParamPath != "":
		return "event_param_path"
	default:
		return "builtin"
	}
}

// friendlyTypeFromForm reads the type-builder fields posted by form.html
// into a friendlyType - either the simple builder fields, or (if the
// advanced type JSON was filled in) the raw-JSON override.
func friendlyTypeFromForm(c *gin.Context) friendlyType {
	if adv := strings.TrimSpace(c.PostForm("advanced_type_json")); adv != "" {
		return friendlyType{UseAdvanced: true, AdvancedType: adv, AdvancedDefault: strings.TrimSpace(c.PostForm("advanced_default_json"))}
	}
	return friendlyType{
		BaseType:    c.PostForm("base_type"),
		IsArray:     c.Request.FormValue("is_array") != "",
		ItemType:    c.PostForm("item_type"),
		IsNullable:  c.Request.FormValue("is_nullable") != "",
		DefaultMode: c.PostForm("default_mode"),
		DefaultText: c.PostForm("default_text"),
	}
}

// formInput reads and validates the posted field/rule form. name comes
// from the URL for edits (readonly in the form) or the posted "name" field
// for new fields.
func formInput(c *gin.Context, name string) (store.SchemaField, store.MappingRule, friendlyType, error) {
	if name == "" {
		name = c.PostForm("name")
	}
	ft := friendlyTypeFromForm(c)
	r := store.MappingRule{Field: name}
	if name == "" {
		return store.SchemaField{}, r, ft, fmt.Errorf("field name is required")
	}

	typeJSON, hasDefault, defaultJSON, err := buildTypeAndDefault(ft)
	if err != nil {
		return store.SchemaField{Name: name}, r, ft, fmt.Errorf("field type: %w", err)
	}
	f := store.SchemaField{Name: name, TypeJSON: typeJSON, HasDefault: hasDefault, DefaultJSON: defaultJSON}

	switch c.PostForm("source_kind") {
	case "builtin":
		r.Builtin = c.PostForm("builtin")
	case "event_param":
		r.EventParam = c.PostForm("event_param")
	case "event_param_path":
		r.EventParamPath = c.PostForm("event_param_path")
	}
	r.Coerce = c.PostForm("coerce")
	if c.Request.FormValue("rule_has_default") != "" {
		r.HasDefault = true
		r.Default = c.PostForm("rule_default")
	}

	return f, r, ft, nil
}

func (h *handlers) create(c *gin.Context) {
	f, r, ft, err := formInput(c, "")
	if err != nil {
		h.renderForm(c, http.StatusBadRequest, gin.H{"IsNew": true, "Field": f, "Rule": r, "FT": ft, "Error": err.Error()})
		return
	}
	if err := h.cfg.Store.UpsertField(f, r); err != nil {
		h.renderForm(c, http.StatusBadRequest, gin.H{"IsNew": true, "Field": f, "Rule": r, "FT": ft, "Error": err.Error()})
		return
	}
	c.Redirect(http.StatusSeeOther, h.url("/?flash="+template.URLQueryEscaper("field added: "+f.Name)))
}

func (h *handlers) update(c *gin.Context) {
	name := c.Param("name")
	f, r, ft, err := formInput(c, name)
	if err != nil {
		h.renderForm(c, http.StatusBadRequest, gin.H{"IsNew": false, "Field": f, "Rule": r, "FT": ft, "Error": err.Error()})
		return
	}
	existing, _, lookupErr := h.cfg.Store.GetField(name)
	if lookupErr == nil && existing != nil {
		f.Position = existing.Position
	}
	if err := h.cfg.Store.UpsertField(f, r); err != nil {
		h.renderForm(c, http.StatusBadRequest, gin.H{"IsNew": false, "Field": f, "Rule": r, "FT": ft, "Error": err.Error()})
		return
	}
	c.Redirect(http.StatusSeeOther, h.url("/?flash="+template.URLQueryEscaper("field saved: "+f.Name)))
}

func (h *handlers) reorder(c *gin.Context) {
	names := c.PostFormArray("selected_fields")
	direction := c.PostForm("direction")
	if err := h.cfg.Store.ReorderBlock(names, direction); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/?flash_class=err&flash="+template.URLQueryEscaper(err.Error())))
		return
	}
	c.Redirect(http.StatusSeeOther, h.url("/?flash="+template.URLQueryEscaper(fmt.Sprintf("moved %d field(s) %s", len(names), direction))))
}

// setOrder applies an exact field ordering, used by drag-and-drop
// reordering: the client computes the full new order client-side and posts
// it directly, rather than a single up/down move. Called via fetch() from
// list.html's JS, so it responds with a small JSON body rather than a
// redirect - the client reloads the page itself once this succeeds.
func (h *handlers) setOrder(c *gin.Context) {
	names := c.PostFormArray("field_order")
	if err := h.cfg.Store.SetFieldOrder(names); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (h *handlers) delete(c *gin.Context) {
	name := c.Param("name")
	if err := h.cfg.Store.DeleteField(name); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/?flash_class=err&flash="+template.URLQueryEscaper("delete failed: "+err.Error())))
		return
	}
	c.Redirect(http.StatusSeeOther, h.url("/?flash="+template.URLQueryEscaper("field deleted: "+name)))
}

func (h *handlers) publish(c *gin.Context) {
	// PublishSnapshot reads schema_fields/mapping_rules exactly once and
	// builds both the schema JSON and the mapping config from that same
	// read, so what's about to be published and what gets snapshotted as
	// "last published" (for Revert) can never drift apart from a
	// concurrent edit landing between two separate reads.
	schemaJSON, mappingCfg, err := h.cfg.Store.PublishSnapshot(h.cfg.SchemaNamespace, h.cfg.SchemaRecordName)
	if err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/?flash_class=err&flash="+template.URLQueryEscaper("publish failed: "+err.Error())))
		return
	}
	codec, err := avroenc.LoadSchema(schemaJSON)
	if err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/?flash_class=err&flash="+template.URLQueryEscaper("publish failed (schema): "+err.Error())))
		return
	}

	// Smoke-test the new mapping+schema against a synthetic sample event
	// before it goes live. Building the schema/mapping only checks that
	// each half parses on its own - neither confirms the two actually fit
	// together (e.g. a coerce mode that doesn't match the field's declared
	// Avro type). Without this, a bad edit publishes cleanly and then
	// every real event fails at codec.EncodeNaked in the hot path, silently
	// dropping 100% of traffic with only a per-event log line and no way
	// for the operator to know from here that anything is wrong.
	//
	// This runs after PublishSnapshot already wrote the new snapshot, so a
	// refused publish here does leave the snapshot slightly ahead of what
	// Publisher.Publish never received - acceptable because Publish itself
	// is refused too (the live server keeps its previous, working config),
	// so there's nothing incorrect actually running; only "revert" would
	// restore to state that was validated but never live, which is exactly
	// what an operator would want to fix forward from anyway.
	if err := smokeTestPublish(mappingCfg, codec); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/?flash_class=err&flash="+template.URLQueryEscaper("publish refused - this mapping/schema combination fails to encode a sample event: "+err.Error())))
		return
	}

	h.cfg.Publisher.Publish(mappingCfg, codec)

	msg := fmt.Sprintf("published: %d fields, %d mapping rules now live", len(mappingCfg.Fields), len(mappingCfg.Fields))
	flashClass := "ok"

	// Push the same schema out to any configured downstream systems
	// (NiFi's parameter context, Druid's supervisor spec) - a failure
	// here does NOT roll back the publish that already succeeded above
	// (this instance is live with the new schema either way); it's
	// reported so an operator knows a downstream system needs manual
	// attention, not silently left stale.
	if h.cfg.PublishSync != nil {
		fields, fErr := h.cfg.Store.ListSchemaFields()
		if fErr != nil {
			msg += " (downstream sync skipped: " + fErr.Error() + ")"
			flashClass = "err"
		} else {
			syncFields := make([]PublishSyncField, len(fields))
			for i, f := range fields {
				syncFields[i] = PublishSyncField{Name: f.Name, TypeJSON: f.TypeJSON}
			}
			syncMsg, syncErr := h.cfg.PublishSync(schemaJSON, syncFields)
			if syncMsg != "" {
				msg += " - " + syncMsg
			}
			if syncErr != nil {
				msg += " - DOWNSTREAM SYNC FAILED: " + syncErr.Error()
				flashClass = "err"
			}
		}
	}

	c.Redirect(http.StatusSeeOther, h.url("/?flash="+template.URLQueryEscaper(msg)+"&flash_class="+flashClass))
}

// smokeTestPublish evaluates mappingCfg against a realistic synthetic
// event and encodes the result with codec, to catch a mapping+schema
// combination that parses fine on its own but doesn't actually fit
// together (e.g. an event_param coerced to a type the field's declared
// Avro type can't accept) before it goes live.
func smokeTestPublish(mappingCfg *mapping.Config, codec *avroenc.Codec) error {
	// A real beacon always carries these (they're part of the base wire
	// protocol, not optional add-ons) - leaving them nil, as a bare
	// &event.BrowserEvent{} does, makes any *builtin* producer sourced from
	// one of them (e.g. location(), referer()) resolve to "absent" exactly
	// like a malformed/incomplete request would, which fails encoding for
	// any such field that's required with no schema default. That's not a
	// realistic "would a normal event break this" case, so populate them.
	dummyStr := "smoketest-value"
	dummyInt := 1
	sample := &event.BrowserEvent{
		EventType:           &dummyStr,
		Location:            &dummyStr,
		Referer:             &dummyStr,
		ViewportPixelWidth:  &dummyInt,
		ViewportPixelHeight: &dummyInt,
		ScreenPixelWidth:    &dummyInt,
		ScreenPixelHeight:   &dummyInt,
		DevicePixelRatio:    &dummyInt,
		RawUserAgent:        "Mozilla/5.0 (smoketest)",
	}

	// A real browser beacon almost always carries a full custom-params
	// blob ("u" param), and at least one required-with-no-default field
	// (namespace, in the real production mapping) is only ever populated
	// from it - eventParameters().value(key) stays genuinely absent when
	// the whole blob is missing (matching legacy, and intentionally so:
	// see internal/mapping's package doc). A synthetic event with no
	// custom params at all is therefore not a realistic "would a normal
	// event break this" test - it reproduces the one case legacy and Go
	// both already correctly reject, and would make this smoke test fail
	// for every real production mapping forever, not just a genuinely
	// broken edit. Populate a dummy value for every event_param/
	// event_param_path key the mapping actually references instead, so a
	// refused publish means the edit itself is broken, not that the
	// sample event is unrealistic.
	customParams := make(map[string]interface{}, len(mappingCfg.Fields))
	for _, rule := range mappingCfg.Fields {
		// A coerced field needs a value that actually parses as that
		// number - "smoketest-value" would fail int32/fp64 parsing,
		// degrading to absent (per mapping.coerce's malformed-input
		// handling) and then failing encoding all over again if that
		// field also happens to be required with no schema default.
		v := interface{}("smoketest-value")
		if rule.Coerce == "int32" || rule.Coerce == "fp64" {
			v = "1"
		}
		if rule.EventParam != "" {
			customParams[rule.EventParam] = v
		}
		if rule.EventParamPath != "" {
			customParams[rule.EventParamPath] = v
		}
	}

	ctx := mapping.NewContext(sample, customParams, false)
	fields, err := mappingCfg.Evaluate(ctx)
	if err != nil {
		return fmt.Errorf("evaluating mapping against a sample event: %w", err)
	}
	if _, err := codec.EncodeNaked(fields); err != nil {
		return fmt.Errorf("encoding a sample event: %w", err)
	}
	return nil
}

// revert discards all edits made since the last Publish (or server boot),
// restoring the store to exactly match what's currently live.
func (h *handlers) revert(c *gin.Context) {
	if err := h.cfg.Store.Revert(); err != nil {
		c.Redirect(http.StatusSeeOther, h.url("/?flash_class=err&flash="+template.URLQueryEscaper("revert failed: "+err.Error())))
		return
	}
	c.Redirect(http.StatusSeeOther, h.url("/?flash="+template.URLQueryEscaper("changes reverted to the last published version")))
}
