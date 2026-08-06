package event

import "testing"

func TestDivolteIdentifierRoundTrip(t *testing.T) {
	id := DivolteIdentifier{TimestampMillis: 1690000000123, ID: "aGVsbG93b3JsZGFiY2RlZmdoaWprbG1u"}
	s := id.String()
	parsed, ok := ParseDivolteIdentifier(s)
	if !ok {
		t.Fatalf("ParseDivolteIdentifier(%q) failed to parse own output", s)
	}
	if parsed != id {
		t.Errorf("round trip mismatch: got %+v, want %+v", parsed, id)
	}
}

func TestParseDivolteIdentifierRejectsWrongVersion(t *testing.T) {
	if _, ok := ParseDivolteIdentifier("1:abc:xyz"); ok {
		t.Errorf("expected version 1 to be rejected")
	}
}

func TestParseDivolteIdentifierRejectsWrongSegmentCount(t *testing.T) {
	cases := []string{"0:abc", "0:abc:xyz:extra", "garbage"}
	for _, c := range cases {
		if _, ok := ParseDivolteIdentifier(c); ok {
			t.Errorf("expected %q to be rejected", c)
		}
	}
}

func TestParseDivolteIdentifierAcceptsOpaqueThirdSegmentVerbatim(t *testing.T) {
	// The Java server never re-validates the 3rd segment as base64 - any
	// content is accepted as-is, including something that isn't valid
	// base64url at all.
	parsed, ok := ParseDivolteIdentifier("0:a:not-valid-base64!!!")
	if !ok {
		t.Fatalf("expected opaque 3rd segment to be accepted")
	}
	if parsed.ID != "not-valid-base64!!!" {
		t.Errorf("ID = %q, want verbatim passthrough", parsed.ID)
	}
}

func TestNewDivolteIdentifierProducesParsableIdentifier(t *testing.T) {
	id, err := NewDivolteIdentifier()
	if err != nil {
		t.Fatalf("NewDivolteIdentifier: %v", err)
	}
	parsed, ok := ParseDivolteIdentifier(id.String())
	if !ok {
		t.Fatalf("ParseDivolteIdentifier(%q) failed on freshly minted identifier", id.String())
	}
	if parsed != id {
		t.Errorf("round trip mismatch: got %+v, want %+v", parsed, id)
	}
	if len(id.ID) != 32 {
		// 24 random bytes, base64url with padding => 32 chars, no '=' needed
		// since 24 is a multiple of 3.
		t.Errorf("ID length = %d, want 32", len(id.ID))
	}
}
