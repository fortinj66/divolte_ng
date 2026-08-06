package event

import (
	"crypto/rand"
	"encoding/base64"
	"strconv"
	"strings"
	"time"
)

// DivolteIdentifier is the party/session identifier format used by the
// browser tag: "0:<base36 epoch millis>:<opaque id>". Ported from
// io.divolte.server.DivolteIdentifier - the 3rd segment is accepted verbatim
// on parse (never re-validated as base64), matching the Java server's
// leniency.
type DivolteIdentifier struct {
	TimestampMillis int64
	ID              string
}

const identifierVersion = "0"

func (d DivolteIdentifier) String() string {
	return identifierVersion + ":" + strconv.FormatInt(d.TimestampMillis, 36) + ":" + d.ID
}

// ParseDivolteIdentifier parses "0:<base36ms>:<id>". Returns ok=false for
// anything else (wrong version, wrong segment count, unparsable timestamp) -
// mirroring DivolteIdentifier.tryParse, which never throws, only returns
// Optional.empty().
func ParseDivolteIdentifier(s string) (DivolteIdentifier, bool) {
	parts := strings.SplitN(s, ":", 4)
	if len(parts) != 3 || parts[0] != identifierVersion {
		return DivolteIdentifier{}, false
	}
	ts, ok := ParseBase36Int64(parts[1])
	if !ok {
		return DivolteIdentifier{}, false
	}
	return DivolteIdentifier{TimestampMillis: ts, ID: parts[2]}, true
}

// NewDivolteIdentifier mints a fresh identifier the way the Java server
// would for a server-minted id: 24 random bytes, URL-safe base64 with
// padding. Not used by the client-side cookie event path (the browser tag
// mints its own), but kept for any future server-side identifier needs.
func NewDivolteIdentifier() (DivolteIdentifier, error) {
	buf := make([]byte, 24)
	if _, err := rand.Read(buf); err != nil {
		return DivolteIdentifier{}, err
	}
	return DivolteIdentifier{
		TimestampMillis: time.Now().UnixMilli(),
		ID:              base64.URLEncoding.EncodeToString(buf),
	}, nil
}
