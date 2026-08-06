package adminui

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

// TestLogoutAlways401s confirms /logout returns 401 (with a
// WWW-Authenticate header) EVEN with valid credentials presented - this
// is what makes a browser actually drop its cached Basic Auth credential,
// since Basic Auth has no logout of its own.
func TestLogoutAlways401s(t *testing.T) {
	handler, _, _ := newTestUI(t)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	// With valid credentials.
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/logout", nil)
	req.SetBasicAuth("admin", "secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /logout (authed): %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("status with valid credentials = %d, want 401", resp.StatusCode)
	}
	if resp.Header.Get("WWW-Authenticate") == "" {
		t.Error("expected a WWW-Authenticate header so the browser re-prompts")
	}

	// With no credentials at all.
	resp2, err := http.Get(ts.URL + "/logout")
	if err != nil {
		t.Fatalf("GET /logout (unauthed): %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusUnauthorized {
		t.Errorf("status with no credentials = %d, want 401", resp2.StatusCode)
	}
}
