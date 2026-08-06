package event

import "testing"

func TestFormatBase36IsLowercase(t *testing.T) {
	got := FormatBase36(1234567890123)
	for _, r := range got {
		if r >= 'A' && r <= 'Z' {
			t.Fatalf("FormatBase36 produced uppercase char in %q", got)
		}
	}
}

func TestParseBase36Int64CaseInsensitive(t *testing.T) {
	lower, ok := ParseBase36Int64("1a2b3c")
	if !ok {
		t.Fatal("lowercase parse failed")
	}
	upper, ok := ParseBase36Int64("1A2B3C")
	if !ok {
		t.Fatal("uppercase parse failed")
	}
	if lower != upper {
		t.Errorf("case-sensitivity mismatch: %d != %d", lower, upper)
	}
}

func TestParseBase36Int64Negative(t *testing.T) {
	v, ok := ParseBase36Int64("-11")
	if !ok || v != -37 {
		t.Errorf("ParseBase36Int64(-11) = %d, %v; want -37, true", v, ok)
	}
}

func TestBase36RoundTrip(t *testing.T) {
	for _, n := range []int64{0, 1, 42, -37, 1690000000123, -1} {
		s := FormatBase36(n)
		got, ok := ParseBase36Int64(s)
		if !ok || got != n {
			t.Errorf("round trip failed for %d: got %d, ok=%v (via %q)", n, got, ok, s)
		}
	}
}

func TestParseBase36IntRejectsOutOfInt32Range(t *testing.T) {
	s := FormatBase36(1 << 40)
	if _, ok := ParseBase36Int(s); ok {
		t.Errorf("expected out-of-int32-range value to be rejected")
	}
}
