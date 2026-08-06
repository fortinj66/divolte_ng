// Command trafficmirror is a transparent tee proxy for validating the Go
// divolte-collector rewrite against real production traffic without any
// risk to production: it sits between HAProxy and the real legacy
// divolte-collector, forwards every request to legacy exactly as before
// (legacy's response is the ONLY one that ever reaches the real client),
// and separately fires a best-effort copy of the same request at a second
// target (typically another node's Go rewrite instance, reached through
// its own HAProxy). The mirror leg never blocks, delays, or can affect the
// primary response in any way - if it errors, times out, or the target is
// unreachable, this process silently drops that one mirrored copy and
// moves on.
package main

import (
	"bytes"
	"context"
	"flag"
	"io"
	"log"
	"net/http"
	"time"
)

// maxBodyBytes bounds how much of a request body this proxy will buffer.
// Beacon requests are tiny (query-string only, GET, no body in practice);
// this cap exists purely so a misconfigured, retried, or malicious oversized
// body can't grow this process's memory unbounded while it sits live in a
// production request path.
const maxBodyBytes = 64 * 1024

// newProxyTransport builds an *http.Transport sized for this proxy's actual
// concurrency, rather than relying on http.DefaultTransport's
// MaxIdleConnsPerHost (2) - which, forwarding real concurrent production
// traffic to a single backend host, would force most requests to pay a
// fresh TCP handshake instead of reusing an idle connection, adding latency
// to the primary (authoritative) leg this proxy must stay transparent for.
func newProxyTransport() *http.Transport {
	t := http.DefaultTransport.(*http.Transport).Clone()
	t.MaxIdleConns = 256
	t.MaxIdleConnsPerHost = 128
	t.IdleConnTimeout = 90 * time.Second
	return t
}

func main() {
	listenAddr := flag.String("listen", "127.0.0.1:18293", "address to listen on (HAProxy's backend should point here)")
	primaryURL := flag.String("primary", "http://127.0.0.1:8290", "primary backend - its response is the only one returned to the real client")
	mirrorURL := flag.String("mirror", "http://collector-02.example.com", "mirror target - a best-effort copy of each request is sent here; its response is discarded")
	primaryTimeout := flag.Duration("primary-timeout", 10*time.Second, "timeout for the primary (authoritative) request")
	mirrorTimeout := flag.Duration("mirror-timeout", 5*time.Second, "timeout for the best-effort mirrored request")
	debugMirror := flag.Bool("debug-mirror", false, "log each mirror attempt's outcome (status/error) - off by default to avoid flooding the log under real traffic volume")
	flag.Parse()

	transport := newProxyTransport()
	primaryClient := &http.Client{Timeout: *primaryTimeout, Transport: transport}
	mirrorClient := &http.Client{Timeout: *mirrorTimeout, Transport: transport}

	handler := &teeHandler{
		primaryBase:   *primaryURL,
		mirrorBase:    *mirrorURL,
		primaryClient: primaryClient,
		mirrorClient:  mirrorClient,
		mirrorTimeout: *mirrorTimeout,
		debugMirror:   *debugMirror,
	}

	srv := &http.Server{
		Addr:              *listenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	log.Printf("trafficmirror listening on %s - primary=%s (authoritative) mirror=%s (best-effort)", *listenAddr, *primaryURL, *mirrorURL)
	if err := srv.ListenAndServe(); err != nil {
		log.Fatalf("listen: %v", err)
	}
}

type teeHandler struct {
	primaryBase   string
	mirrorBase    string
	primaryClient *http.Client
	mirrorClient  *http.Client
	mirrorTimeout time.Duration
	debugMirror   bool
}

func (h *teeHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// The request body (if any - GET beacon requests never have one, but
	// this stays correct for any future POST use) needs to be readable
	// twice: once for the primary leg, once for the mirror. Buffer it
	// fully upfront; beacon requests are tiny so this is cheap. Capped at
	// maxBodyBytes so an oversized/malicious body can't grow this
	// continuously-running process's memory unbounded.
	var body []byte
	if r.Body != nil {
		var err error
		body, err = io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
		r.Body.Close()
		if err != nil {
			http.Error(w, "reading request body", http.StatusBadGateway)
			return
		}
		if len(body) > maxBodyBytes {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
	}

	// Fire the mirror copy in the background - it must never affect the
	// primary response, so it gets its own timeout-bounded context
	// independent of the client's request context (which ends as soon as
	// the primary response is written).
	go h.mirror(r, body)

	h.proxyPrimary(w, r, body)
}

func (h *teeHandler) proxyPrimary(w http.ResponseWriter, r *http.Request, body []byte) {
	req, err := http.NewRequestWithContext(r.Context(), r.Method, h.primaryBase+r.URL.RequestURI(), newBodyReader(body))
	if err != nil {
		http.Error(w, "building primary request", http.StatusBadGateway)
		return
	}
	copyHeaders(req.Header, r.Header)
	req.Host = r.Host

	resp, err := h.primaryClient.Do(req)
	if err != nil {
		log.Printf("primary request failed: %v", err)
		http.Error(w, "primary backend unreachable", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	copyHeaders(w.Header(), resp.Header)
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("copying primary response body: %v", err)
	}
}

func (h *teeHandler) mirror(r *http.Request, body []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), h.mirrorTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, r.Method, h.mirrorBase+r.URL.RequestURI(), newBodyReader(body))
	if err != nil {
		return
	}
	copyHeaders(req.Header, r.Header)
	// Preserve the original Host header - the mirror target's own HAProxy
	// routes on it (e.g. a virtual-host-based hostname), and rewriting
	// it to this proxy's own host would misroute the mirrored copy.
	req.Host = r.Host

	resp, err := h.mirrorClient.Do(req)
	if err != nil {
		// Best-effort: not logged by default to avoid flooding this
		// process's log under real traffic volume. Mirror failures are
		// expected occasionally (target restart, network blip) and are
		// never actionable from here. -debug-mirror surfaces them anyway
		// when actively validating the mirror path.
		if h.debugMirror {
			log.Printf("mirror request failed: %v", err)
		}
		return
	}
	defer resp.Body.Close()
	if h.debugMirror {
		log.Printf("mirror response: %d %s", resp.StatusCode, req.URL)
	}
	io.Copy(io.Discard, resp.Body)
}

// newBodyReader returns a fresh reader over body each time it's called -
// the primary and mirror requests each need their own independent read
// position over the same underlying bytes.
func newBodyReader(body []byte) io.Reader {
	if len(body) == 0 {
		return nil
	}
	return bytes.NewReader(body)
}

// hopByHopHeaders are the connection-specific headers RFC 7230 section 6.1
// says must not be forwarded by a proxy - net/http/httputil.ReverseProxy
// strips the same set. Since this proxy fully buffers the body rather than
// streaming it, forwarding Transfer-Encoding/Connection verbatim can create
// framing ambiguity between this process, HAProxy, and the backends.
var hopByHopHeaders = []string{
	"Connection", "Proxy-Connection", "Keep-Alive", "Transfer-Encoding",
	"TE", "Trailer", "Upgrade", "Proxy-Authenticate", "Proxy-Authorization",
}

// copyHeaders copies src into dst, omitting hop-by-hop headers and any
// headers additionally named in src's own Connection header.
func copyHeaders(dst, src http.Header) {
	skip := make(map[string]bool, len(hopByHopHeaders))
	for _, h := range hopByHopHeaders {
		skip[h] = true
	}
	for _, v := range src.Values("Connection") {
		skip[http.CanonicalHeaderKey(v)] = true
	}
	for k, vs := range src {
		if skip[http.CanonicalHeaderKey(k)] {
			continue
		}
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
}
