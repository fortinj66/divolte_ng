// Package event implements the wire protocol for divolte.js's browser event
// beacon (the "csc-event" GET request): query-param extraction, the
// DivolteIdentifier format, base-36 encoding, and the MurmurHash3-based
// checksum - ported to be byte-compatible with an unmodified deployed
// divolte.js tag hitting a new Go server, per io.divolte.server.
// ClientSideCookieEventHandler.
package event

import (
	"fmt"
	"net/url"
)

// BrowserEvent is a parsed event beacon request. Optional fields are nil
// when absent, mirroring the Java server's Optional<T> semantics - the
// mapping layer decides what to do with absence (e.g. the production
// mapping substitutes "" for an absent Referer), rather than this package
// baking in a default.
type BrowserEvent struct {
	PartyID   DivolteIdentifier
	SessionID DivolteIdentifier

	PageViewID string
	EventID    string

	IsNewParty       bool
	IsFirstInSession bool

	ClientTimestampMillis int64

	// ChecksumCorrect is false both when the checksum is wrong AND when the
	// "x" param is missing/unparsable - it never causes the request to be
	// rejected, only flags the resulting record as "corrupt" downstream.
	ChecksumCorrect bool

	EventType           *string
	CustomParamsRaw     *string // mincode-encoded "u" param; decode via internal/mincode
	Location            *string
	Referer             *string
	ViewportPixelWidth  *int
	ViewportPixelHeight *int
	ScreenPixelWidth    *int
	ScreenPixelHeight   *int
	DevicePixelRatio    *int

	RemoteHost string

	// The following are not part of the query string - the HTTP layer
	// populates them after ParseBrowserBeacon returns, since they come from
	// the raw request (receipt time, User-Agent header) rather than
	// client-supplied query params.

	// ReceivedAtMillis is the server's own receipt timestamp, matching
	// Divolte's `timestamp()` mapping producer (distinct from the client's
	// own clock, carried in the "c" param as ClientTimestampMillis).
	ReceivedAtMillis int64

	// RawUserAgent is the standard HTTP "User-Agent" request header,
	// matching Divolte's `userAgentString()` producer.
	RawUserAgent string
}

// ParseBrowserBeacon extracts a BrowserEvent from the beacon request's
// decoded query parameters (equivalent to Undertow's
// exchange.getQueryParameters(), i.e. already URL-decoded - use
// url.ParseQuery on the raw query string, or an http.Request's URL.Query()).
//
// Returns an error if any of the required params (p, s, n, f, e, c, v) are
// missing or unparsable, mirroring IncompleteRequestException in the Java
// server - the caller must still serve the GIF/304 response as normal (that
// happens before parsing in the original implementation) and simply drop
// the event without enqueuing it for mapping/Kafka.
func ParseBrowserBeacon(values url.Values, remoteHost string) (*BrowserEvent, error) {
	first := func(key string) (string, bool) {
		vs, ok := values[key]
		if !ok || len(vs) == 0 {
			return "", false
		}
		return vs[0], true
	}

	pStr, ok := first("p")
	if !ok {
		return nil, fmt.Errorf("event: missing required param p")
	}
	partyID, ok := ParseDivolteIdentifier(pStr)
	if !ok {
		return nil, fmt.Errorf("event: invalid partyId %q", pStr)
	}

	sStr, ok := first("s")
	if !ok {
		return nil, fmt.Errorf("event: missing required param s")
	}
	sessionID, ok := ParseDivolteIdentifier(sStr)
	if !ok {
		return nil, fmt.Errorf("event: invalid sessionId %q", sStr)
	}

	nStr, ok := first("n")
	if !ok {
		return nil, fmt.Errorf("event: missing required param n")
	}
	fStr, ok := first("f")
	if !ok {
		return nil, fmt.Errorf("event: missing required param f")
	}
	eStr, ok := first("e")
	if !ok {
		return nil, fmt.Errorf("event: missing required param e")
	}
	cStr, ok := first("c")
	if !ok {
		return nil, fmt.Errorf("event: missing required param c")
	}
	cMillis, ok := ParseBase36Int64(cStr)
	if !ok {
		return nil, fmt.Errorf("event: invalid client timestamp %q", cStr)
	}
	vStr, ok := first("v")
	if !ok {
		return nil, fmt.Errorf("event: missing required param v")
	}

	ev := &BrowserEvent{
		PartyID:               partyID,
		SessionID:             sessionID,
		PageViewID:            vStr,
		EventID:               eStr,
		IsNewParty:            nStr == "t",
		IsFirstInSession:      fStr == "t",
		ClientTimestampMillis: cMillis,
		ChecksumCorrect:       VerifyChecksum(values),
		RemoteHost:            remoteHost,
	}

	if s, ok := first("t"); ok {
		ev.EventType = &s
	}
	if s, ok := first("u"); ok {
		ev.CustomParamsRaw = &s
	}
	if s, ok := first("l"); ok {
		ev.Location = &s
	}
	if s, ok := first("r"); ok {
		ev.Referer = &s
	}
	if s, ok := first("w"); ok {
		if n, ok := ParseBase36Int(s); ok {
			ev.ViewportPixelWidth = &n
		}
	}
	if s, ok := first("h"); ok {
		if n, ok := ParseBase36Int(s); ok {
			ev.ViewportPixelHeight = &n
		}
	}
	if s, ok := first("i"); ok {
		if n, ok := ParseBase36Int(s); ok {
			ev.ScreenPixelWidth = &n
		}
	}
	if s, ok := first("j"); ok {
		if n, ok := ParseBase36Int(s); ok {
			ev.ScreenPixelHeight = &n
		}
	}
	if s, ok := first("k"); ok {
		if n, ok := ParseBase36Int(s); ok {
			ev.DevicePixelRatio = &n
		}
	}

	return ev, nil
}
