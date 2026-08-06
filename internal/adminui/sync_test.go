package adminui

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/example/divolte-rewrite/internal/store"
)

func newTestUIWithSync(t *testing.T, publishSync PublishSyncFunc, nifiTest, druidTest SyncTestFunc) (http.Handler, *fakePublisher) {
	t.Helper()
	s := newTestStore(t)
	if err := s.EnsureAdminSettingsSeeded("admin", "secret"); err != nil {
		t.Fatalf("seeding admin settings: %v", err)
	}
	if err := s.UpsertField(
		store.SchemaField{Name: "partyId", TypeJSON: `"string"`},
		store.MappingRule{Builtin: "partyId"},
	); err != nil {
		t.Fatalf("seeding field: %v", err)
	}
	pub := &fakePublisher{}
	handler, err := New(Config{
		Store: s, Publisher: pub,
		SchemaNamespace: "test.record", SchemaRecordName: "trimmed",
		PublishSync: publishSync,
		NiFiTest:    nifiTest,
		DruidTest:   druidTest,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return handler, pub
}

// TestPublishIncludesSyncResultInFlash confirms a successful PublishSync
// result gets appended to the normal "published: N fields..." flash
// message, and the flash stays "ok" styled.
func TestPublishIncludesSyncResultInFlash(t *testing.T) {
	handler, _ := newTestUIWithSync(t, func(schemaJSON string, fields []PublishSyncField) (string, error) {
		if schemaJSON == "" {
			t.Error("PublishSync received an empty schemaJSON")
		}
		if len(fields) == 0 {
			t.Error("PublishSync received no fields")
		}
		return "NiFi schema updated; Druid dimensions added (foo)", nil
	}, nil, nil)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	client := newAuthedClient(t)

	resp := doAuthed(t, client, ts, "POST", "/publish", nil)
	defer resp.Body.Close()
	loc, err := resp.Location()
	if err != nil {
		t.Fatalf("Location: %v", err)
	}
	q := loc.Query()
	if q.Get("flash_class") == "err" {
		t.Errorf("flash_class = err, want ok/ unset - PublishSync succeeded")
	}
	if !containsAll(q.Get("flash"), "published:", "NiFi schema updated", "Druid dimensions added") {
		t.Errorf("flash = %q, missing expected sync summary content", q.Get("flash"))
	}
}

// TestPublishMarksFlashErrWhenSyncFails confirms a PublishSync failure
// still lets the local publish stand (this instance is already live with
// the new schema - PublishSync failing doesn't roll that back), but
// flags the flash as an error so the failure is visible, not silently
// swallowed.
func TestPublishMarksFlashErrWhenSyncFails(t *testing.T) {
	handler, pub := newTestUIWithSync(t, func(schemaJSON string, fields []PublishSyncField) (string, error) {
		return "NiFi schema updated; Druid FAILED: connection refused", errors.New("1 of 2 downstream sync target(s) failed")
	}, nil, nil)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	client := newAuthedClient(t)

	resp := doAuthed(t, client, ts, "POST", "/publish", nil)
	defer resp.Body.Close()

	if pub.calls != 1 {
		t.Errorf("Publisher.Publish called %d times, want 1 (local publish must still happen even if downstream sync will fail)", pub.calls)
	}

	loc, err := resp.Location()
	if err != nil {
		t.Fatalf("Location: %v", err)
	}
	q := loc.Query()
	if q.Get("flash_class") != "err" {
		t.Errorf("flash_class = %q, want err", q.Get("flash_class"))
	}
	if !containsAll(q.Get("flash"), "published:", "DOWNSTREAM SYNC FAILED") {
		t.Errorf("flash = %q, missing expected failure content", q.Get("flash"))
	}
}

// TestPublishWithNilPublishSyncUnaffected confirms every existing
// deployment (no PublishSync configured) publishes exactly as before
// this feature existed.
func TestPublishWithNilPublishSyncUnaffected(t *testing.T) {
	handler, pub := newTestUIWithSync(t, nil, nil, nil)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	client := newAuthedClient(t)

	resp := doAuthed(t, client, ts, "POST", "/publish", nil)
	defer resp.Body.Close()
	if pub.calls != 1 {
		t.Errorf("Publisher.Publish called %d times, want 1", pub.calls)
	}
	loc, _ := resp.Location()
	if loc.Query().Get("flash_class") == "err" {
		t.Error("flash_class should not be err with no PublishSync configured")
	}
}

func TestNiFiTargetTestReturnsInlineResult(t *testing.T) {
	var gotBaseURL string
	handler, _ := newTestUIWithSync(t, nil, func(v map[string]string) (string, error) {
		gotBaseURL = v["base_url"]
		return "connected", nil
	}, nil)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	client := newAuthedClient(t)

	resp := doAuthed(t, client, ts, "POST", "/nifi-targets/nifi-02/test", url.Values{
		"base_url": {"https://nifi-01.example.com:9443"},
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if gotBaseURL != "https://nifi-01.example.com:9443" {
		t.Errorf("NiFiTest received base_url = %q", gotBaseURL)
	}
}

func TestDruidTargetTestReturnsInlineResult(t *testing.T) {
	var gotSupervisor string
	handler, _ := newTestUIWithSync(t, nil, nil, func(v map[string]string) (string, error) {
		gotSupervisor = v["supervisor_name"]
		return "connected", nil
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()
	client := newAuthedClient(t)

	resp := doAuthed(t, client, ts, "POST", "/druid-targets/druid-dev/test", url.Values{
		"supervisor_name": {"example-web-metrics"},
	})
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if gotSupervisor != "example-web-metrics" {
		t.Errorf("DruidTest received supervisor_name = %q", gotSupervisor)
	}
}

func TestNiFiAndDruidTargetTestWithoutTestFuncConfigured(t *testing.T) {
	handler, _ := newTestUIWithSync(t, nil, nil, nil)
	ts := httptest.NewServer(handler)
	defer ts.Close()
	client := newAuthedClient(t)

	for _, path := range []string{"/nifi-targets/some-id/test", "/druid-targets/some-id/test"} {
		resp := doAuthed(t, client, ts, "POST", path, url.Values{})
		body := readBody(t, resp)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Errorf("%s status = %d, want 200", path, resp.StatusCode)
		}
		if !containsAll(body, "not available") {
			t.Errorf("%s body = %q, want a message indicating it isn't available", path, body)
		}
	}
}

func containsAll(s string, subs ...string) bool {
	for _, sub := range subs {
		if !strings.Contains(s, sub) {
			return false
		}
	}
	return true
}
