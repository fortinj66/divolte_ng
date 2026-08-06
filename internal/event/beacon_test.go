package event

import (
	"net/url"
	"testing"
)

// buildBeaconQuery constructs a url.Values the way a real divolte.js client
// would, computing the checksum over everything except "x" itself - the
// same canonicalization the server independently recomputes.
func buildBeaconQuery(fields map[string]string) url.Values {
	values := url.Values{}
	for k, v := range fields {
		values.Set(k, v)
	}
	checksum := ComputeChecksum(values)
	values.Set("x", FormatBase36(int64(checksum)))
	return values
}

func TestParseBrowserBeaconHappyPath(t *testing.T) {
	party := DivolteIdentifier{TimestampMillis: 1690000000000, ID: "party-id-opaque-value"}
	session := DivolteIdentifier{TimestampMillis: 1690000001000, ID: "session-id-opaque-value"}

	values := buildBeaconQuery(map[string]string{
		"p": party.String(),
		"s": session.String(),
		"n": "t",
		"f": "f",
		"e": "evt-1",
		"c": FormatBase36(1690000002000),
		"v": "pageview-1",
		"t": "pageView",
		"l": "https://example.com/page",
		"r": "https://example.com/referrer",
		"w": FormatBase36(1920),
		"h": FormatBase36(1080),
		"i": FormatBase36(1920),
		"j": FormatBase36(1080),
		"k": FormatBase36(2),
	})

	ev, err := ParseBrowserBeacon(values, "203.0.113.5")
	if err != nil {
		t.Fatalf("ParseBrowserBeacon: %v", err)
	}

	if ev.PartyID != party {
		t.Errorf("PartyID = %+v, want %+v", ev.PartyID, party)
	}
	if ev.SessionID != session {
		t.Errorf("SessionID = %+v, want %+v", ev.SessionID, session)
	}
	if !ev.IsNewParty {
		t.Error("IsNewParty = false, want true")
	}
	if ev.IsFirstInSession {
		t.Error("IsFirstInSession = true, want false")
	}
	if ev.ClientTimestampMillis != 1690000002000 {
		t.Errorf("ClientTimestampMillis = %d, want 1690000002000", ev.ClientTimestampMillis)
	}
	if !ev.ChecksumCorrect {
		t.Error("ChecksumCorrect = false, want true (checksum was computed correctly)")
	}
	if ev.EventType == nil || *ev.EventType != "pageView" {
		t.Errorf("EventType = %v, want pageView", ev.EventType)
	}
	if ev.ViewportPixelWidth == nil || *ev.ViewportPixelWidth != 1920 {
		t.Errorf("ViewportPixelWidth = %v, want 1920", ev.ViewportPixelWidth)
	}
	if ev.DevicePixelRatio == nil || *ev.DevicePixelRatio != 2 {
		t.Errorf("DevicePixelRatio = %v, want 2", ev.DevicePixelRatio)
	}
	if ev.RemoteHost != "203.0.113.5" {
		t.Errorf("RemoteHost = %q, want 203.0.113.5", ev.RemoteHost)
	}
	// u/CustomParamsRaw wasn't set in this test - should be absent (nil).
	if ev.CustomParamsRaw != nil {
		t.Errorf("CustomParamsRaw = %v, want nil (absent)", *ev.CustomParamsRaw)
	}
}

func TestParseBrowserBeaconTamperedChecksumIsNonFatal(t *testing.T) {
	party := DivolteIdentifier{TimestampMillis: 1, ID: "p"}
	session := DivolteIdentifier{TimestampMillis: 2, ID: "s"}
	values := buildBeaconQuery(map[string]string{
		"p": party.String(), "s": session.String(), "n": "t", "f": "t",
		"e": "evt", "c": FormatBase36(1000), "v": "pv",
	})
	// Tamper with a param after the checksum was computed, without
	// recomputing "x" - simulates a proxy truncating/corrupting the request.
	values.Set("v", "tampered-page-view-id")

	ev, err := ParseBrowserBeacon(values, "127.0.0.1")
	if err != nil {
		t.Fatalf("ParseBrowserBeacon should not error on a bad checksum: %v", err)
	}
	if ev.ChecksumCorrect {
		t.Error("ChecksumCorrect = true, want false after tampering")
	}
	if ev.PageViewID != "tampered-page-view-id" {
		t.Errorf("event should still be parsed and used despite bad checksum")
	}
}

func TestParseBrowserBeaconMissingChecksumIsNonFatal(t *testing.T) {
	values := url.Values{}
	values.Set("p", DivolteIdentifier{TimestampMillis: 1, ID: "p"}.String())
	values.Set("s", DivolteIdentifier{TimestampMillis: 2, ID: "s"}.String())
	values.Set("n", "t")
	values.Set("f", "t")
	values.Set("e", "evt")
	values.Set("c", FormatBase36(1000))
	values.Set("v", "pv")
	// no "x" param at all

	ev, err := ParseBrowserBeacon(values, "127.0.0.1")
	if err != nil {
		t.Fatalf("missing checksum param should not error: %v", err)
	}
	if ev.ChecksumCorrect {
		t.Error("ChecksumCorrect = true, want false when x is absent")
	}
}

func TestParseBrowserBeaconRequiresPartyID(t *testing.T) {
	values := url.Values{}
	values.Set("s", DivolteIdentifier{TimestampMillis: 2, ID: "s"}.String())
	values.Set("n", "t")
	values.Set("f", "t")
	values.Set("e", "evt")
	values.Set("c", FormatBase36(1000))
	values.Set("v", "pv")

	if _, err := ParseBrowserBeacon(values, "127.0.0.1"); err == nil {
		t.Error("expected error when p (partyId) is missing")
	}
}

func TestParseBrowserBeaconRequiresParsableTimestamp(t *testing.T) {
	values := url.Values{}
	values.Set("p", DivolteIdentifier{TimestampMillis: 1, ID: "p"}.String())
	values.Set("s", DivolteIdentifier{TimestampMillis: 2, ID: "s"}.String())
	values.Set("n", "t")
	values.Set("f", "t")
	values.Set("e", "evt")
	values.Set("c", "not-base36-parsable-!!!")
	values.Set("v", "pv")

	if _, err := ParseBrowserBeacon(values, "127.0.0.1"); err == nil {
		t.Error("expected error when c (client timestamp) is unparsable")
	}
}
