package httpserver

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/IBM/sarama"

	"github.com/example/divolte-rewrite/internal/avroenc"
	"github.com/example/divolte-rewrite/internal/event"
	"github.com/example/divolte-rewrite/internal/kafkasink"
	"github.com/example/divolte-rewrite/internal/mapping"
	"github.com/example/divolte-rewrite/internal/mincode"
)

func mincodeEncodeQuantity(n int64) (string, error) {
	return mincode.Encode(map[string]interface{}{"quantity": n})
}

const testSchema = `{
  "namespace": "test.record", "type": "record", "name": "trimmed",
  "fields": [
    { "name": "detectedDuplicate", "type": "boolean" },
    { "name": "partyId", "type": "string" },
    { "name": "quantity", "type": ["int", "null"], "default": 0 }
  ]
}`

// fakeProducer implements kafkasink.Producer and reports each sent message
// on a channel, so tests can deterministically wait for the async
// mapping/encode/publish pipeline to finish without sleeping-and-hoping.
type fakeProducer struct {
	sent chan *sarama.ProducerMessage
}

func newFakeProducer() *fakeProducer {
	return &fakeProducer{sent: make(chan *sarama.ProducerMessage, 10)}
}

func (f *fakeProducer) SendMessage(msg *sarama.ProducerMessage) (int32, int64, error) {
	f.sent <- msg
	return 0, 0, nil
}

func (f *fakeProducer) Close() error { return nil }

func newTestServer(t *testing.T) (*Server, http.Handler, *fakeProducer) {
	t.Helper()
	codec, err := avroenc.LoadSchema(testSchema)
	if err != nil {
		t.Fatalf("LoadSchema: %v", err)
	}
	mappingCfg := &mapping.Config{Fields: []mapping.FieldRule{
		{Field: "detectedDuplicate", Builtin: "duplicate"},
		{Field: "partyId", Builtin: "partyId"},
		{Field: "quantity", EventParam: "quantity", Coerce: "int32"},
	}}
	fp := newFakeProducer()
	sink := kafkasink.NewWithProducer(fp, "test-topic")
	mgr := kafkasink.NewSingleSinkManager("test", "avro", sink)

	srv, handler := New(Config{
		Prefix: "/webstats/", ScriptName: "divolte_ng.js", EventSuffix: "csc-event",
		MappingCfg: mappingCfg, Codec: codec, Sink: mgr,
		Workers: 1, QueueSize: 100, DuplicateMemorySize: 1000,
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Close(ctx)
	})
	return srv, handler, fp
}

func TestPingEndpoint(t *testing.T) {
	_, handler, _ := newTestServer(t)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/ping")
	if err != nil {
		t.Fatalf("GET /ping: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200", resp.StatusCode)
	}
}

func TestPingFailsAfterPrepareShutdown(t *testing.T) {
	// Matches legacy's shutdown ordering (Server.java: pingHandler.shutdown()
	// fails the health check before the real drain starts) - a load
	// balancer polling /ping should see failure and stop routing here
	// before connections actually start closing.
	srv, handler, _ := newTestServer(t)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	srv.PrepareShutdown()

	resp, err := http.Get(ts.URL + "/ping")
	if err != nil {
		t.Fatalf("GET /ping: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want 503 after PrepareShutdown", resp.StatusCode)
	}
}

func TestEventBeaconRejectsNonGetMethods(t *testing.T) {
	// Matches legacy's AllowedMethodsHandler wrapping the event endpoint
	// (BrowserSource.java) - a real browser only ever GETs the beacon.
	_, handler, fp := newTestServer(t)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	for _, method := range []string{http.MethodPost, http.MethodPut, http.MethodDelete} {
		req, err := http.NewRequest(method, buildValidBeaconURL(t, ts.URL), nil)
		if err != nil {
			t.Fatalf("building %s request: %v", method, err)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s csc-event: %v", method, err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusMethodNotAllowed {
			t.Errorf("%s status = %d, want 405", method, resp.StatusCode)
		}
	}

	select {
	case msg := <-fp.sent:
		t.Errorf("expected no Kafka publish for a rejected method, got %v", msg)
	case <-time.After(200 * time.Millisecond):
		// expected: nothing published
	}
}

func TestScriptServedWithETagAndGzip(t *testing.T) {
	_, handler, _ := newTestServer(t)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/webstats/divolte_ng.js")
	if err != nil {
		t.Fatalf("GET divolte_ng.js: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	etag := resp.Header.Get("ETag")
	if etag == "" {
		t.Fatal("expected an ETag header")
	}

	req, _ := http.NewRequest("GET", ts.URL+"/webstats/divolte_ng.js", nil)
	req.Header.Set("If-None-Match", etag)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("conditional GET: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotModified {
		t.Errorf("conditional GET status = %d, want 304", resp2.StatusCode)
	}
}

func TestScriptOverrideServedWhenPresent(t *testing.T) {
	dir := t.TempDir()
	overrideBody := []byte("override-marker-content")
	if err := os.WriteFile(filepath.Join(dir, "divolte_ng.js"), overrideBody, 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	codec, err := avroenc.LoadSchema(testSchema)
	if err != nil {
		t.Fatalf("LoadSchema: %v", err)
	}
	fp := newFakeProducer()
	sink := kafkasink.NewWithProducer(fp, "test-topic")
	mgr := kafkasink.NewSingleSinkManager("test", "avro", sink)
	srv, handler := New(Config{
		Prefix: "/webstats/", ScriptName: "divolte_ng.js", EventSuffix: "csc-event",
		StaticOverrideDir: dir,
		MappingCfg:        &mapping.Config{},
		Codec:             codec,
		Sink:              mgr,
		Workers:           1, QueueSize: 100, DuplicateMemorySize: 1000,
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Close(ctx)
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/webstats/divolte_ng.js")
	if err != nil {
		t.Fatalf("GET divolte_ng.js: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if string(body) != string(overrideBody) {
		t.Errorf("body = %q, want the override content %q", body, overrideBody)
	}
}

func TestScriptOverrideFallsBackWhenAbsent(t *testing.T) {
	// StaticOverrideDir is a real, existing directory, but it has no file
	// named after ScriptName in it - must fall back to the embedded asset,
	// not error or serve an empty body.
	dir := t.TempDir()
	codec, err := avroenc.LoadSchema(testSchema)
	if err != nil {
		t.Fatalf("LoadSchema: %v", err)
	}
	fp := newFakeProducer()
	sink := kafkasink.NewWithProducer(fp, "test-topic")
	mgr := kafkasink.NewSingleSinkManager("test", "avro", sink)
	srv, handler := New(Config{
		Prefix: "/webstats/", ScriptName: "divolte_ng.js", EventSuffix: "csc-event",
		StaticOverrideDir: dir,
		MappingCfg:        &mapping.Config{},
		Codec:             codec,
		Sink:              mgr,
		Workers:           1, QueueSize: 100, DuplicateMemorySize: 1000,
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = srv.Close(ctx)
	})
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/webstats/divolte_ng.js")
	if err != nil {
		t.Fatalf("GET divolte_ng.js: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	if len(body) == 0 {
		t.Fatal("expected the embedded fallback script body, got empty response")
	}
}

func buildValidBeaconURL(t *testing.T, base string) string {
	t.Helper()
	party := event.DivolteIdentifier{TimestampMillis: 1, ID: "party"}
	session := event.DivolteIdentifier{TimestampMillis: 2, ID: "session"}
	fields := map[string]string{
		"p": party.String(), "s": session.String(), "n": "t", "f": "t",
		"e": "evt-1", "c": event.FormatBase36(1690000000000), "v": "pv-1",
	}
	values := url.Values{}
	for k, v := range fields {
		values.Set(k, v)
	}
	return base + "/webstats/csc-event?" + values.Encode()
}

func TestEventBeaconHappyPathPublishesToKafka(t *testing.T) {
	_, handler, fp := newTestServer(t)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	resp, err := http.Get(buildValidBeaconURL(t, ts.URL))
	if err != nil {
		t.Fatalf("GET csc-event: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "image/gif" {
		t.Errorf("Content-Type = %q, want image/gif", ct)
	}

	select {
	case msg := <-fp.sent:
		if msg.Topic != "test-topic" {
			t.Errorf("Topic = %q, want test-topic", msg.Topic)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the event to reach the Kafka producer")
	}
}

func TestEventBeaconRepeatedIfNoneMatchReturns304WithoutPublishing(t *testing.T) {
	_, handler, fp := newTestServer(t)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	beaconURL := buildValidBeaconURL(t, ts.URL)
	// First fire - consume the resulting Kafka message so we know
	// processing genuinely happened.
	resp1, err := http.Get(beaconURL)
	if err != nil {
		t.Fatalf("GET csc-event (first): %v", err)
	}
	resp1.Body.Close()
	select {
	case <-fp.sent:
	case <-time.After(2 * time.Second):
		t.Fatal("first request never reached kafka")
	}

	req, _ := http.NewRequest("GET", beaconURL, nil)
	req.Header.Set("If-None-Match", `"6b3edc43-20ec-4078-bc47-e965dd76b88a"`)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("conditional GET: %v", err)
	}
	defer resp2.Body.Close()
	if resp2.StatusCode != http.StatusNotModified {
		t.Errorf("status = %d, want 304", resp2.StatusCode)
	}

	select {
	case msg := <-fp.sent:
		t.Errorf("expected no second Kafka publish for a 304 duplicate re-fire, got %v", msg)
	case <-time.After(200 * time.Millisecond):
		// expected: nothing published
	}
}

func TestPublishHotSwapsMappingWithoutRestart(t *testing.T) {
	srv, handler, fp := newTestServer(t)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	// First fire with the original mapping (quantity comes from the
	// "quantity" event param).
	url1 := buildValidBeaconURL(t, ts.URL) + "&u=" + urlEncodeMincodeQuantity(t, 5)
	resp1, err := http.Get(url1)
	if err != nil {
		t.Fatalf("GET csc-event (before publish): %v", err)
	}
	resp1.Body.Close()

	var before *sarama.ProducerMessage
	select {
	case before = <-fp.sent:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the pre-publish event")
	}
	if before == nil {
		t.Fatal("nil message before publish")
	}

	// Now publish a mapping where "quantity" always resolves to a fixed
	// literal via a different event param key ("other_qty") - if the old
	// mapping were still in effect, this event's "quantity" custom param
	// would be ignored since the field now reads from a different key.
	newMappingCfg := &mapping.Config{Fields: []mapping.FieldRule{
		{Field: "detectedDuplicate", Builtin: "duplicate"},
		{Field: "partyId", Builtin: "partyId"},
		{Field: "quantity", EventParam: "other_qty", Coerce: "int32"},
	}}
	codec, err := avroenc.LoadSchema(testSchema)
	if err != nil {
		t.Fatalf("LoadSchema: %v", err)
	}
	srv.Publish(newMappingCfg, codec)

	// Fire again with the OLD event param key ("quantity") populated but
	// not "other_qty" - under the new mapping this should NOT populate the
	// quantity field (it'll be absent/default), proving the swap took effect.
	url2 := buildValidBeaconURL(t, ts.URL) + "&u=" + urlEncodeMincodeQuantity(t, 99)
	resp2, err := http.Get(url2)
	if err != nil {
		t.Fatalf("GET csc-event (after publish): %v", err)
	}
	resp2.Body.Close()

	select {
	case after := <-fp.sent:
		decoded, err := codec.DecodeNaked(after.Value.(sarama.ByteEncoder))
		if err != nil {
			t.Fatalf("decoding post-publish record: %v", err)
		}
		// quantity has schema default 0 - under the new mapping, the old
		// "quantity" event param (99) must NOT have populated it.
		if fmt := decoded["quantity"]; fmt == int32(99) {
			t.Errorf("quantity = %v, want NOT 99 - the new mapping (reading other_qty) should not see the old param", fmt)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for the post-publish event")
	}
}

func urlEncodeMincodeQuantity(t *testing.T, n int64) string {
	t.Helper()
	encoded, err := mincodeEncodeQuantity(n)
	if err != nil {
		t.Fatalf("encoding quantity: %v", err)
	}
	return url.QueryEscape(encoded)
}

func TestEventBeaconIncompleteRequestStillServesGIF(t *testing.T) {
	_, handler, fp := newTestServer(t)
	ts := httptest.NewServer(handler)
	defer ts.Close()

	// Missing required params (p, s, etc.) - the GIF must still be served,
	// the event is just silently dropped afterward.
	resp, err := http.Get(ts.URL + "/webstats/csc-event?e=only-event-id")
	if err != nil {
		t.Fatalf("GET csc-event: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Errorf("status = %d, want 200 even for an incomplete request", resp.StatusCode)
	}

	select {
	case msg := <-fp.sent:
		t.Errorf("expected no Kafka publish for an incomplete request, got %v", msg)
	case <-time.After(200 * time.Millisecond):
		// expected: nothing published
	}
}
