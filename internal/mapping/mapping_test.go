package mapping

import (
	"testing"

	"github.com/example/divolte-rewrite/internal/event"
)

func testEvent() *event.BrowserEvent {
	party := event.DivolteIdentifier{TimestampMillis: 1, ID: "party"}
	session := event.DivolteIdentifier{TimestampMillis: 2, ID: "session"}
	loc := "https://example.com/page"
	return &event.BrowserEvent{
		PartyID:               party,
		SessionID:             session,
		PageViewID:            "pv-1",
		EventID:               "evt-1",
		IsFirstInSession:      true,
		ClientTimestampMillis: 1690000000000,
		ChecksumCorrect:       true,
		Location:              &loc,
		RemoteHost:            "203.0.113.5",
		ReceivedAtMillis:      1690000000500,
		RawUserAgent:          "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
	}
}

func TestEvaluateBuiltinsAndEventParams(t *testing.T) {
	cfg := &Config{Fields: []FieldRule{
		{Field: "detectedDuplicate", Builtin: "duplicate"},
		{Field: "detectedCorruption", Builtin: "corrupt"},
		{Field: "timestamp", Builtin: "timestamp"},
		{Field: "partyId", Builtin: "partyId"},
		{Field: "userAgentFamily", Builtin: "userAgent.family"},
		{Field: "referer", Builtin: "referer", Default: strPtr("")},
		{Field: "quantity", EventParam: "quantity", Coerce: "int32"},
		{Field: "sku_id", EventParamPath: "sku_id"},
		{Field: "missing_field", EventParam: "does_not_exist"},
		{Field: "missing_coerced_field", EventParam: "also_does_not_exist", Coerce: "int32"},
	}}

	ctx := NewContext(testEvent(), map[string]interface{}{
		"quantity": int64(3),
		"sku_id":   []interface{}{"sku-a", "sku-b"},
	}, false)

	out, err := cfg.Evaluate(ctx)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}

	if out["detectedDuplicate"] != false {
		t.Errorf("detectedDuplicate = %v, want false", out["detectedDuplicate"])
	}
	if out["detectedCorruption"] != false {
		t.Errorf("detectedCorruption = %v, want false (checksum was correct)", out["detectedCorruption"])
	}
	if out["timestamp"] != int64(1690000000500) {
		t.Errorf("timestamp = %v, want server ReceivedAtMillis, not client ClientTimestampMillis", out["timestamp"])
	}
	if out["partyId"] != "0:1:party" {
		t.Errorf("partyId = %v, want 0:1:party", out["partyId"])
	}
	if out["userAgentFamily"] != "Chrome" {
		t.Errorf("userAgentFamily = %v, want Chrome", out["userAgentFamily"])
	}
	if out["referer"] != "" {
		t.Errorf("referer = %v, want empty-string default (absent + default)", out["referer"])
	}
	if out["quantity"] != int32(3) {
		t.Errorf("quantity = %v (%T), want int32(3)", out["quantity"], out["quantity"])
	}
	skus, ok := out["sku_id"].([]interface{})
	if !ok || len(skus) != 2 {
		t.Errorf("sku_id = %v, want 2-element slice", out["sku_id"])
	}
	// A missing plain event_param (no Coerce) resolves to "" rather than
	// absent, matching legacy's eventParameters().value(key) - backed by
	// Jackson's JsonNode.path(key).asText(), which never signals "missing"
	// (confirmed against a real legacy instance via cmd/paritycheck; this
	// is what makes fields like namespace/user_email - required, no
	// schema default - survive a request that omits them instead of
	// dropping the whole event).
	if out["missing_field"] != "" {
		t.Errorf("missing_field = %v, want \"\" (event_param defaults to empty string, matching legacy's .value() semantics)", out["missing_field"])
	}
	// A missing COERCED event_param is a different case - there's no raw
	// value to coerce, so it stays absent/defaulted rather than trying to
	// int32-parse "".
	if _, present := out["missing_coerced_field"]; present {
		t.Errorf("missing_coerced_field should be omitted (absent, no default), got %v", out["missing_coerced_field"])
	}
}

func TestEvaluateEventParamAbsentWhenCustomParamsBlobEntirelyMissing(t *testing.T) {
	// A plain event_param (no Coerce) defaults to "" when the custom-params
	// blob is present but lacks that specific key (see
	// TestEvaluateBuiltinsAndEventParams's "missing_field" case) - but when
	// the WHOLE blob is absent (no "u" param on the request at all, e.g. a
	// bare pageview beacon), legacy's eventParameters() producer itself
	// never resolves, so .value(key)'s map() chain never runs and the field
	// stays genuinely absent. Confirmed against a real, isolated legacy
	// instance via cmd/paritycheck: a plain-pageview fixture with no custom
	// params still got dropped by legacy for missing "namespace" - the
	// first version of this fix wrongly defaulted to "" here too, which
	// made the Go rewrite publish events legacy would have dropped.
	cfg := &Config{Fields: []FieldRule{
		{Field: "partyId", Builtin: "partyId"},
		{Field: "namespace", EventParam: "namespace"},
	}}
	ctx := NewContext(testEvent(), nil, false)

	out, err := cfg.Evaluate(ctx)
	if err != nil {
		t.Fatalf("Evaluate: %v", err)
	}
	if _, present := out["namespace"]; present {
		t.Errorf("namespace = %v, want absent when CustomParams is nil entirely (matches legacy dropping these)", out["namespace"])
	}
}

func TestEvaluateCoerceFailureTreatedAsAbsentNotFatal(t *testing.T) {
	// Real clients occasionally send a value of the wrong shape for a
	// coerced field (e.g. an array where product_quantity expects a
	// scalar int32). Matching legacy's parse(...).to(int32) semantics
	// (a failed parse leaves the field absent, not the whole record
	// unbuildable), this must NOT fail the whole Evaluate() call - just
	// that one field should end up absent/defaulted.
	cfg := &Config{Fields: []FieldRule{
		{Field: "partyId", Builtin: "partyId"},
		{Field: "product_quantity", EventParam: "product_quantity", Coerce: "int32"},
	}}

	ctx := NewContext(testEvent(), map[string]interface{}{
		"product_quantity": []interface{}{int64(1), int64(2)},
	}, false)

	out, err := cfg.Evaluate(ctx)
	if err != nil {
		t.Fatalf("Evaluate: %v, want no error - a coercion failure should degrade gracefully", err)
	}
	if out["partyId"] != "0:1:party" {
		t.Errorf("partyId = %v, want 0:1:party (rest of the record unaffected)", out["partyId"])
	}
	if _, present := out["product_quantity"]; present {
		t.Errorf("product_quantity = %v, want absent (falls back to schema default)", out["product_quantity"])
	}
}

func strPtr(s string) *string { return &s }
