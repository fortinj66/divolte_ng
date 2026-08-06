package kafkasink

import (
	"context"
	"fmt"
	"log"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/example/divolte-rewrite/internal/avroenc"
	"github.com/example/divolte-rewrite/internal/jsonenc"
)

// TargetSpec describes one Kafka output target for Manager.Reconcile -
// deliberately a package-local type (not internal/store.KafkaOutputTarget)
// so kafkasink doesn't depend on internal/store, matching how
// internal/nifiavro/internal/druid take their own Config types and main.go
// converts store rows into them. Reconcile only ever sees targets that are
// meant to be active - filter out disabled ones before calling it.
type TargetSpec struct {
	ID      string
	Format  string // "avro" | "json"
	Topic   string
	Brokers []string
}

func (t TargetSpec) contentKey() string {
	return t.Format + "|" + t.Topic + "|" + strings.Join(t.Brokers, ",")
}

type runtimeTarget struct {
	spec TargetSpec
	sink *Sink
}

// Manager fans a single mapped event out to N independently configured
// Kafka sinks (some Avro, some JSON), hot-swappable via Reconcile with no
// process restart - the live set is held behind an atomic pointer the same
// way internal/httpserver's liveConfig is, since Publish is called once per
// incoming event (high frequency) and can't afford to take a lock or hit
// the DB per call the way the NiFi/Druid sync targets do on each Publish
// click (low frequency, tolerates that).
type Manager struct {
	baseClientID string

	mu      sync.Mutex // serializes Reconcile calls only, never the Publish hot path
	live    atomic.Pointer[[]runtimeTarget]
	newSink func(Config) (*Sink, error) // overridden in tests
}

// NewManager returns an empty Manager - call Reconcile to populate it.
// baseClientID is this instance's own Kafka client ID (e.g.
// "divolte-collector-go-d02"); each target's actual producer client ID is
// baseClientID + "-" + target ID, so target rows never need to carry a
// client ID of their own.
func NewManager(baseClientID string) *Manager {
	m := &Manager{baseClientID: baseClientID, newSink: New}
	empty := []runtimeTarget{}
	m.live.Store(&empty)
	return m
}

// NewSingleSinkManager wraps an already-constructed Sink as the sole live
// target - for callers/tests that build a Sink directly (e.g. via
// NewWithProducer against a mock producer) and don't need Reconcile's
// content-diffing/hot-swap machinery, which requires real broker
// connectivity to open a producer.
func NewSingleSinkManager(id, format string, sink *Sink) *Manager {
	m := &Manager{baseClientID: id, newSink: New}
	targets := []runtimeTarget{{spec: TargetSpec{ID: id, Format: format}, sink: sink}}
	m.live.Store(&targets)
	return m
}

// Reconcile brings the live sink set in line with specs: unchanged targets
// (same ID AND same format/topic/brokers) keep their already-open producer
// untouched - an edit to one target must never bounce another's live
// connection, since d02/d03's real production topic is one of these
// targets. New or changed targets get a freshly opened producer; targets no
// longer present are closed. Safe to call concurrently with itself (e.g.
// from two admin UI requests) - not safe to skip calling after every
// create/update/delete, since Publish only ever sees what Reconcile last
// stored.
func (m *Manager) Reconcile(specs []TargetSpec) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	currentByID := make(map[string]runtimeTarget)
	for _, rt := range *m.live.Load() {
		currentByID[rt.spec.ID] = rt
	}

	next := make([]runtimeTarget, 0, len(specs))
	var toClose []*Sink

	for _, spec := range specs {
		existing, ok := currentByID[spec.ID]
		delete(currentByID, spec.ID)
		if ok && existing.spec.contentKey() == spec.contentKey() {
			next = append(next, existing)
			continue
		}
		sink, err := m.newSink(Config{
			Brokers:  spec.Brokers,
			ClientID: m.baseClientID + "-" + spec.ID,
			Topic:    spec.Topic,
		})
		if err != nil {
			return fmt.Errorf("kafkasink: reconciling target %q: %w", spec.ID, err)
		}
		next = append(next, runtimeTarget{spec: spec, sink: sink})
		if ok {
			toClose = append(toClose, existing.sink)
		}
	}
	// Anything still in currentByID was removed or disabled.
	for _, rt := range currentByID {
		toClose = append(toClose, rt.sink)
	}

	m.live.Store(&next)

	for _, s := range toClose {
		if err := s.Close(); err != nil {
			log.Printf("kafkasink: closing replaced/removed producer: %v", err)
		}
	}
	return nil
}

// Publish encodes fields once per format actually needed by a live target
// (not once per target) and sends to every live target concurrently, so
// one down/slow sink's bounded retry (see Sink.Send) doesn't stack in
// series behind another's. JSON is encoded first, from the pristine
// fields map, because avroenc.Codec.EncodeNaked mutates fields in place -
// if any Avro target is live, that mutation must happen last. Returns one
// error per failed target (nil slice if everything succeeded, or if there
// are no live targets) - matching the existing "log and drop, never fail
// the request" semantics; the caller is expected to log, not retry.
func (m *Manager) Publish(ctx context.Context, partyID string, codec *avroenc.Codec, fields map[string]interface{}) []error {
	targets := *m.live.Load()
	if len(targets) == 0 {
		return nil
	}

	var needJSON, needAvro bool
	for _, t := range targets {
		switch t.spec.Format {
		case "json":
			needJSON = true
		case "avro":
			needAvro = true
		}
	}

	var jsonBytes, avroBytes []byte
	if needJSON {
		b, err := jsonenc.Encode(fields)
		if err != nil {
			return []error{fmt.Errorf("kafkasink: json encoding: %w", err)}
		}
		jsonBytes = b
	}
	if needAvro {
		b, err := codec.EncodeNaked(fields)
		if err != nil {
			return []error{fmt.Errorf("kafkasink: avro encoding: %w", err)}
		}
		avroBytes = b
	}

	errs := make([]error, len(targets))
	var wg sync.WaitGroup
	for i, t := range targets {
		var payload []byte
		switch t.spec.Format {
		case "json":
			payload = jsonBytes
		case "avro":
			payload = avroBytes
		default:
			errs[i] = fmt.Errorf("kafkasink: target %q: unknown format %q", t.spec.ID, t.spec.Format)
			continue
		}
		wg.Add(1)
		go func(i int, t runtimeTarget, payload []byte) {
			defer wg.Done()
			if err := t.sink.Send(ctx, partyID, payload); err != nil {
				errs[i] = fmt.Errorf("target %q: %w", t.spec.ID, err)
			}
		}(i, t, payload)
	}
	wg.Wait()

	var out []error
	for _, e := range errs {
		if e != nil {
			out = append(out, e)
		}
	}
	return out
}

// Close shuts down every live producer.
func (m *Manager) Close() error {
	var firstErr error
	for _, t := range *m.live.Load() {
		if err := t.sink.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}
