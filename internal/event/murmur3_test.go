package event

import "testing"

// Expected values captured by running divolte.js's own murmum3_32() function
// (copied verbatim from reference/divolte-collector/src/main/resources/divolte.js
// lines ~299-363) under Node.js for these exact inputs, seed 0. This proves
// byte-for-byte agreement between the Go server's checksum computation and
// what a real deployed divolte.js client computes.
func TestMurmur3_32MatchesDivolteJS(t *testing.T) {
	cases := []struct {
		input string
		want  uint32
	}{
		{"", 0},
		{"test", 3127628307},
		{"abc", 3017643002},
		{"abcd", 1139631978},
		{"abcde", 3902511862},
		{"abcdefgh", 1239272644},
		{"p=1,;s=2,;x=hello,world,;", 2503423244},
	}
	for _, c := range cases {
		got := Murmur3_32([]byte(c.input), 0)
		if got != c.want {
			t.Errorf("murmur3_32(%q, 0) = %d, want %d", c.input, got, c.want)
		}
	}
}
