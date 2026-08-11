package adminui

import (
	"io"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

// TestBrandingOverrideServedWhenPresent locks in the mount-your-own-logo
// story: BrandingDir lets a deployment swap the compiled-in placeholder
// logo/favicon for its own files without a rebuild.
func TestBrandingOverrideServedWhenPresent(t *testing.T) {
	s := newTestStore(t)
	if err := s.EnsureAdminSettingsSeeded("admin", "secret"); err != nil {
		t.Fatalf("seeding admin settings: %v", err)
	}

	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "logo.svg"), []byte("override-logo-content"), 0o644); err != nil {
		t.Fatalf("WriteFile logo.svg: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "favicon.ico"), []byte("override-favicon-content"), 0o644); err != nil {
		t.Fatalf("WriteFile favicon.ico: %v", err)
	}

	pub := &fakePublisher{}
	handler, err := New(Config{
		Store: s, Publisher: pub,
		SchemaNamespace: "test.record", SchemaRecordName: "trimmed",
		BrandingDir: dir,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ts := httptest.NewServer(handler)
	defer ts.Close()
	client := newAuthedClient(t)

	resp := doAuthed(t, client, ts, "GET", "/static/logo.svg", nil)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "override-logo-content" {
		t.Errorf("logo.svg body = %q, want the override content", body)
	}

	resp = doAuthed(t, client, ts, "GET", "/static/favicon.ico", nil)
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "override-favicon-content" {
		t.Errorf("favicon.ico body = %q, want the override content", body)
	}
}

// TestBrandingOverrideFallsBackWhenAbsent covers a BrandingDir that exists
// but has neither file in it - must fall back to the embedded defaults, not
// error or 404.
func TestBrandingOverrideFallsBackWhenAbsent(t *testing.T) {
	s := newTestStore(t)
	if err := s.EnsureAdminSettingsSeeded("admin", "secret"); err != nil {
		t.Fatalf("seeding admin settings: %v", err)
	}

	dir := t.TempDir()

	pub := &fakePublisher{}
	handler, err := New(Config{
		Store: s, Publisher: pub,
		SchemaNamespace: "test.record", SchemaRecordName: "trimmed",
		BrandingDir: dir,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ts := httptest.NewServer(handler)
	defer ts.Close()
	client := newAuthedClient(t)

	resp := doAuthed(t, client, ts, "GET", "/static/logo.svg", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("logo.svg status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(body) == 0 {
		t.Fatal("expected the embedded fallback logo, got empty response")
	}

	resp = doAuthed(t, client, ts, "GET", "/static/favicon.ico", nil)
	if resp.StatusCode != 200 {
		t.Fatalf("favicon.ico status = %d, want 200", resp.StatusCode)
	}
	body, _ = io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(body) == 0 {
		t.Fatal("expected the embedded fallback favicon, got empty response")
	}
}
