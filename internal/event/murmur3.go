package event

import (
	"encoding/binary"
	"math/bits"
)

// Murmur3_32 is the standard MurmurHash3 x86 32-bit algorithm, matching
// Guava's Hashing.murmur3_32() (used server-side by divolte-collector) and
// the hand-rolled implementation in divolte.js (used client-side) - both
// default to seed 0. Cross-validated against the exact divolte.js source in
// murmur3_test.go. Exported for reuse by internal/dedupe, which needs the
// same primitive for an unrelated (non-wire) hashing purpose.
func Murmur3_32(data []byte, seed uint32) uint32 {
	const c1 = 0xcc9e2d51
	const c2 = 0x1b873593

	h1 := seed
	nblocks := len(data) / 4
	for i := 0; i < nblocks; i++ {
		k1 := binary.LittleEndian.Uint32(data[i*4:])
		k1 *= c1
		k1 = bits.RotateLeft32(k1, 15)
		k1 *= c2
		h1 ^= k1
		h1 = bits.RotateLeft32(h1, 13)
		h1 = h1*5 + 0xe6546b64
	}

	tail := data[nblocks*4:]
	var k1 uint32
	switch len(tail) {
	case 3:
		k1 ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k1 ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k1 ^= uint32(tail[0])
		k1 *= c1
		k1 = bits.RotateLeft32(k1, 15)
		k1 *= c2
		h1 ^= k1
	}

	h1 ^= uint32(len(data))
	h1 ^= h1 >> 16
	h1 *= 0x85ebca6b
	h1 ^= h1 >> 13
	h1 *= 0xc2b2ae35
	h1 ^= h1 >> 16
	return h1
}
