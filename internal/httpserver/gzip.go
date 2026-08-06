package httpserver

import (
	"bytes"
	"compress/gzip"
)

// gzipBytes pre-compresses the tracking tag once at startup (mirroring the
// Java server caching both a plain and pre-gzipped form of the compiled
// tag), so serving it never re-compresses per-request.
func gzipBytes(data []byte) ([]byte, bool) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		return nil, false
	}
	if err := w.Close(); err != nil {
		return nil, false
	}
	return buf.Bytes(), true
}
