// Package dedupe ports divolte-collector's ShortTermDuplicateMemory: a
// fixed-size, probabilistic duplicate detector. Each (partyId, sessionId,
// eventId) triple hashes to a slot in a fixed-size array; a stored
// "signature" in that slot is compared against the new key's signature -
// a match means "probably a duplicate" (a proxy/client retried the same
// event), a mismatch overwrites the slot and reports "not a duplicate".
// False positives/negatives are possible by design (bounded memory, not a
// full history), same trade-off the original makes.
//
// Unlike the wire protocol packages (event, mincode), the exact hash
// algorithm here is NOT part of any external contract - Divolte's
// "duplicate" mapping field is a boolean outcome, not a value observed by
// any client or other system, so bit-for-bit matching Java's internal
// murmur3_128 usage isn't required for compatibility. This uses two
// independently-seeded 32-bit hashes (reusing the already wire-validated
// murmur3_32 from internal/event's checksum logic) combined into a slot
// index + 64-bit signature, which preserves the same behavioral contract
// (bounded memory, probabilistic detection) without the added complexity
// and risk of a hand-rolled 128-bit algorithm that has no observable
// output to validate against.
//
// Instances are NOT safe for concurrent use - matching the original, each
// is meant to be owned by a single worker (see internal/pool), so that
// dedupe state naturally partitions along the same partyId-affinity
// routing used for per-party ordering.
package dedupe

import (
	"encoding/binary"

	"github.com/example/divolte-rewrite/internal/event"
)

// Memory is a fixed-size probabilistic duplicate detector.
type Memory struct {
	slots []uint64
}

// New creates a Memory with the given number of slots (Divolte's default is
// 1,000,000, configurable via mapper.duplicate_memory_size).
func New(capacity int) *Memory {
	if capacity <= 0 {
		capacity = 1_000_000
	}
	return &Memory{slots: make([]uint64, capacity)}
}

// IsProbableDuplicate reports whether (partyID, sessionID, eventID) has
// likely been seen before, and records it as seen (overwriting the slot)
// when it has not.
func (m *Memory) IsProbableDuplicate(partyID, sessionID, eventID string) bool {
	// Hash each component independently and combine the digests, rather
	// than concatenating with a separator byte - none of partyID/sessionID/
	// eventID are validated against containing that separator verbatim
	// after URL-decoding, so a literal separator byte in one component
	// could otherwise make two genuinely distinct triples collide.
	key := combinedKey(partyID, sessionID, eventID)
	slotHash := event.Murmur3_32(key, 0)
	sigLow := event.Murmur3_32(key, 1)
	sigHigh := event.Murmur3_32(key, 2)
	signature := uint64(sigLow) | uint64(sigHigh)<<32

	idx := int(slotHash) % len(m.slots)
	if idx < 0 {
		idx += len(m.slots)
	}

	existing := m.slots[idx]
	if existing == signature && existing != 0 {
		return true
	}
	m.slots[idx] = signature
	return false
}

// combinedKey unambiguously combines three strings by hashing each one
// separately (fixed-width output) and concatenating those digests, rather
// than concatenating the raw strings with a separator that isn't guaranteed
// absent from the input.
func combinedKey(a, b, c string) []byte {
	buf := make([]byte, 12)
	binary.BigEndian.PutUint32(buf[0:4], event.Murmur3_32([]byte(a), 0))
	binary.BigEndian.PutUint32(buf[4:8], event.Murmur3_32([]byte(b), 0))
	binary.BigEndian.PutUint32(buf[8:12], event.Murmur3_32([]byte(c), 0))
	return buf
}
