package event

import (
	"net/url"
	"sort"
	"strings"
)

// checksumQueryParam is excluded from its own canonicalization.
const checksumQueryParam = "x"

// ComputeChecksum reproduces ClientSideCookieEventHandler's checksum
// canonicalization: sort all query params except "x" by key, and for each
// key render "key=v1,v2,...,;" (a trailing comma after every value,
// including the last), concatenating all key-groups with no separator.
// Hashed with MurmurHash3_x86_32, seed 0. The result is compared, sign-
// extended to 64 bits, against the base-36 "x" param - a mismatch only sets
// a "corrupt" flag, it never rejects the request.
//
// Exported so tooling that needs to construct or verify a real beacon
// checksum (cmd/paritycheck, cmd/smoketest) can call the real
// implementation directly instead of maintaining their own
// re-derivations, which would silently go stale if this logic ever changes.
func ComputeChecksum(values url.Values) int32 {
	keys := make([]string, 0, len(values))
	for k := range values {
		if k == checksumQueryParam {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var sb strings.Builder
	for _, k := range keys {
		sb.WriteString(k)
		sb.WriteByte('=')
		for _, v := range values[k] {
			sb.WriteString(v)
			sb.WriteByte(',')
		}
		sb.WriteByte(';')
	}
	return int32(Murmur3_32([]byte(sb.String()), 0))
}

// VerifyChecksum reports whether the "x" query param matches the computed
// checksum of the rest of the (decoded) query params. Returns false (not an
// error) if "x" is absent/unparsable - same as an incorrect checksum, this
// only ever results in a "corrupt" flag, never a rejected request.
func VerifyChecksum(values url.Values) bool {
	xs, ok := values[checksumQueryParam]
	if !ok || len(xs) == 0 {
		return false
	}
	expected, ok := ParseBase36Int64(xs[0])
	if !ok {
		return false
	}
	computed := ComputeChecksum(values)
	return int64(computed) == expected
}
