package event

import (
	"math"
	"strconv"
	"strings"
)

// FormatBase36 always emits lowercase, matching both Java's Long.toString(n,36)
// and JS's Number.prototype.toString(36) (strconv.FormatInt is already
// lowercase, so this just documents the guarantee).
func FormatBase36(n int64) string {
	return strconv.FormatInt(n, 36)
}

// ParseBase36Int64 parses a base-36 integer, accepting either case (Java's
// Long.parseLong(s,36) is case-insensitive on input even though it only ever
// emits lowercase) and an optional leading '-'.
func ParseBase36Int64(s string) (int64, bool) {
	v, err := strconv.ParseInt(strings.ToLower(s), 36, 64)
	return v, err == nil
}

// ParseBase36Int is like ParseBase36Int64 but bounds-checked to fit a signed
// 32-bit int, matching the beacon's viewport/screen/pixel-ratio fields which
// the Java server parses with Integer.valueOf(s,36).
func ParseBase36Int(s string) (int, bool) {
	v, ok := ParseBase36Int64(s)
	if !ok || v < math.MinInt32 || v > math.MaxInt32 {
		return 0, false
	}
	return int(v), true
}
