// Package httpserver implements the browser source's HTTP surface: serving
// the tracking tag, handling the event beacon per the exact protocol in
// internal/event, and a /ping health check - ported from BrowserSource.java
// and ClientSideCookieEventHandler.java.
//
// Deferred, not implemented: legacy's JSON HTTP event source (JsonSource/
// JsonEventHandler/JsonContentHandler.java - an alternate ingestion API
// that accepts events as a JSON POST body instead of the JS-tag beacon).
// Confirmed real, working legacy code, but no source of type "json" is
// configured in any real deployment we've needed to support so far - the
// browser beacon is the only ingestion path in use. Build this if a real
// need for it shows up, against real traffic that needs it.
package httpserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/example/divolte-rewrite/assets"
	"github.com/example/divolte-rewrite/internal/avroenc"
	"github.com/example/divolte-rewrite/internal/dedupe"
	"github.com/example/divolte-rewrite/internal/event"
	"github.com/example/divolte-rewrite/internal/kafkasink"
	"github.com/example/divolte-rewrite/internal/mapping"
	"github.com/example/divolte-rewrite/internal/mincode"
	"github.com/example/divolte-rewrite/internal/pool"
)

// sentinelETag is a fixed, unvarying ETag advertised on every beacon
// response - the same trick the Java server uses so that a browser's own
// HTTP cache conditionally-revalidates (sends If-None-Match) on any retry
// of the exact same beacon URL, letting the server respond 304 and skip
// re-logging the event. It never changes per-request, matching
// ClientSideCookieEventHandler.SENTINEL_ETAG.
const sentinelETag = `"6b3edc43-20ec-4078-bc47-e965dd76b88a"`

// Config configures one Server instance.
type Config struct {
	Prefix      string // e.g. "/webstats/"
	ScriptName  string // e.g. "divolte_ng.js"
	EventSuffix string // e.g. "csc-event"

	// StaticOverrideDir, if set, is checked for a file named ScriptName -
	// when present, its content is served instead of the compiled-in
	// assets.DivolteNGJS. See internal/config.Config.StaticOverrideDir's
	// doc comment for the full rationale.
	StaticOverrideDir string

	MappingCfg *mapping.Config
	Codec      *avroenc.Codec
	Sink       *kafkasink.Manager

	Workers             int
	QueueSize           int
	DuplicateMemorySize int
}

// liveConfig is the swappable part of a Server's state - the mapping rules
// and Avro schema currently in effect. Publish atomically replaces it, so
// an in-flight event finishes with whichever liveConfig it already loaded
// (no torn reads, no restart needed).
type liveConfig struct {
	mappingCfg *mapping.Config
	codec      *avroenc.Codec
}

// Server holds everything needed to build the http.Handler and to shut
// down the processing pool cleanly.
type Server struct {
	cfg  Config
	live atomic.Pointer[liveConfig]

	jsBody    []byte
	jsGzip    []byte
	jsETag    string
	jsHasGzip bool

	dedupeMemories []*dedupe.Memory
	procPool       *pool.Pool

	draining atomic.Bool

	unparsableCount  atomic.Int64
	badItemTypeCount atomic.Int64

	// sendCtx bounds in-flight Kafka retry backoff waits (internal/kafkasink
	// Sink.Send selects on it) - cancelled at the start of Close so a
	// worker still inside a retry loop when shutdown begins stops promptly
	// instead of sleeping through the rest of its retry budget.
	sendCtx       context.Context
	cancelSendCtx context.CancelFunc
}

// logEveryNDrops mirrors internal/pool's own sampling constant: logging
// every single unparsable/malformed request is itself expensive precisely
// under the traffic conditions (scans, bot floods) that produce the most of
// them.
const logEveryNDrops = 100

// New builds a Server and its http.Handler. Call Close to drain the
// processing pool before shutting down the HTTP listener (mirrors the
// original's "stop upstream before downstream" shutdown ordering - the
// caller should stop accepting new HTTP connections, then call Close).
func New(cfg Config) (*Server, http.Handler) {
	s := &Server{cfg: cfg}
	s.live.Store(&liveConfig{mappingCfg: cfg.MappingCfg, codec: cfg.Codec})
	s.sendCtx, s.cancelSendCtx = context.WithCancel(context.Background())

	s.jsBody = assets.DivolteNGJS
	if cfg.StaticOverrideDir != "" {
		if overrideBody, err := os.ReadFile(filepath.Join(cfg.StaticOverrideDir, cfg.ScriptName)); err == nil {
			s.jsBody = overrideBody
		}
	}
	sum := sha256.Sum256(s.jsBody)
	s.jsETag = `"` + hex.EncodeToString(sum[:]) + `"`
	if gz, ok := gzipBytes(s.jsBody); ok {
		s.jsGzip = gz
		s.jsHasGzip = true
	}

	s.dedupeMemories = make([]*dedupe.Memory, cfg.Workers)
	for i := range s.dedupeMemories {
		s.dedupeMemories[i] = dedupe.New(cfg.DuplicateMemorySize)
	}
	s.procPool = pool.New(pool.Config{
		Workers:    cfg.Workers,
		BufferSize: cfg.QueueSize,
	}, s.processEvent)

	mux := http.NewServeMux()
	mux.HandleFunc(cfg.Prefix+cfg.ScriptName, s.handleScript)
	mux.HandleFunc(cfg.Prefix+cfg.EventSuffix, s.handleEvent)
	mux.HandleFunc("/ping", s.handlePing)
	return s, mux
}

// Publish atomically swaps the mapping config and Avro schema in effect
// for all subsequently-dequeued events - no restart, no dropped in-flight
// requests. Satisfies the Publisher interface internal/adminui uses to
// apply an edit made through the web UI.
func (s *Server) Publish(mappingCfg *mapping.Config, codec *avroenc.Codec) {
	s.live.Store(&liveConfig{mappingCfg: mappingCfg, codec: codec})
}

// PrepareShutdown flips /ping to start failing immediately, before any
// connection draining begins - matching legacy's shutdown ordering
// (Server.java: pingHandler.shutdown() fails the health check, then a
// sleep, and only then does the real drain start). Call this first, then
// wait out the load balancer's health-check interval, then proceed with
// the HTTP server's own Shutdown and this Server's Close - giving the
// load balancer a chance to stop routing new traffic here before
// connections actually start closing.
func (s *Server) PrepareShutdown() {
	s.draining.Store(true)
}

// Close drains the processing pool (finishing in-flight mapping/encode/
// Kafka-send work) and closes the Kafka sink, up to ctx's deadline.
func (s *Server) Close(ctx context.Context) error {
	s.cancelSendCtx()
	if err := s.procPool.Stop(ctx); err != nil {
		return err
	}
	return s.cfg.Sink.Close()
}

func (s *Server) handlePing(w http.ResponseWriter, r *http.Request) {
	if s.draining.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("shutting down"))
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("pong"))
}

func (s *Server) handleScript(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
	w.Header().Set("ETag", s.jsETag)
	w.Header().Set("Cache-Control", "public, max-age=3600")

	if inm := r.Header.Get("If-None-Match"); inm == s.jsETag {
		w.WriteHeader(http.StatusNotModified)
		return
	}

	body := s.jsBody
	if s.jsHasGzip && strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		w.Header().Set("Content-Encoding", "gzip")
		body = s.jsGzip
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_, _ = w.Write(body)
	}
}

// handleEvent implements the event beacon protocol exactly:
// ClientSideCookieEventHandler always serves the GIF/304 response first,
// and only afterward (off the response path) parses and enqueues the
// event - a parse failure never changes what the browser already got back.
func (s *Server) handleEvent(w http.ResponseWriter, r *http.Request) {
	// BrowserSource.java wraps the event handler in an AllowedMethodsHandler
	// restricted to GET, rejecting anything else with 405 before it ever
	// reaches the mapping/encode/Kafka pipeline - real browsers only ever
	// GET the beacon, so this mainly keeps scanners/misconfigured clients
	// from being processed as if they were real events.
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	values := r.URL.Query()

	w.Header().Set("Content-Type", "image/gif")
	w.Header().Set("ETag", sentinelETag)
	w.Header().Set("Cache-Control", "private, no-cache, proxy-revalidate")
	w.Header().Set("Pragma", "no-cache")
	w.Header().Set("Expires", "Fri, 14 Apr 1995 11:30:00 GMT")

	if r.Header.Get("If-None-Match") == sentinelETag {
		w.WriteHeader(http.StatusNotModified)
		return // duplicate re-fire of the same cached beacon URL - do not log again
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(assets.Transparent1x1GIF)

	ev, err := event.ParseBrowserBeacon(values, remoteHost(r))
	if err != nil {
		if n := s.unparsableCount.Add(1); n%logEveryNDrops == 1 {
			log.Printf("httpserver: dropping incomplete beacon request from %s: %v (%d total so far)", r.RemoteAddr, err, n)
		}
		return
	}
	ev.ReceivedAtMillis = time.Now().UnixMilli()
	ev.RawUserAgent = r.Header.Get("User-Agent")

	s.procPool.Enqueue(pool.Item{AffinityKey: ev.PartyID.String(), Value: ev})
}

// processEvent is the pool.Handler for the mapping/dedupe/encode/publish
// stage - workerIndex selects this worker's own dedupe.Memory, matching
// the original's per-processor-thread duplicate memory.
func (s *Server) processEvent(workerIndex int, item pool.Item) {
	ev, ok := item.Value.(*event.BrowserEvent)
	if !ok {
		if n := s.badItemTypeCount.Add(1); n%logEveryNDrops == 1 {
			log.Printf("httpserver: dropping pool item with unexpected value type %T (%d total so far)", item.Value, n)
		}
		return
	}
	live := s.live.Load()

	duplicate := s.dedupeMemories[workerIndex].IsProbableDuplicate(
		ev.PartyID.String(), ev.SessionID.String(), ev.EventID)

	var customParams map[string]interface{}
	if ev.CustomParamsRaw != nil {
		decoded, err := mincode.Decode(*ev.CustomParamsRaw)
		if err != nil {
			log.Printf("httpserver: event %s: mincode decode failed, proceeding without custom params: %v", ev.EventID, err)
		} else if m, ok := decoded.(map[string]interface{}); ok {
			customParams = m
		}
	}

	ctx := mapping.NewContext(ev, customParams, duplicate)
	fields, err := live.mappingCfg.Evaluate(ctx)
	if err != nil {
		log.Printf("httpserver: event %s: mapping failed, dropping: %v", ev.EventID, err)
		return
	}

	if errs := s.cfg.Sink.Publish(s.sendCtx, ev.PartyID.String(), live.codec, fields); len(errs) > 0 {
		for _, err := range errs {
			log.Printf("httpserver: event %s: kafka send failed: %v", ev.EventID, err)
		}
	}
}

// remoteHost extracts the client host (no port) from the request, matching
// remoteHost() - production's config doesn't set use_x_forwarded_for for
// this source, so this reads the direct connection address rather than any
// proxy header.
func remoteHost(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
