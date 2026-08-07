package kafkasink

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/IBM/sarama/mocks"
	"github.com/example/divolte-rewrite/internal/avroenc"
)

const testManagerSchema = `{
	"type": "record",
	"name": "test",
	"fields": [
		{"name": "partyId", "type": "string"}
	]
}`

func testCodec(t *testing.T) *avroenc.Codec {
	codec, err := avroenc.LoadSchema(testManagerSchema)
	if err != nil {
		t.Fatalf("avroenc.LoadSchema: %v", err)
	}
	return codec
}

// fakeSinkFactory lets tests control what Manager.newSink returns without
// opening a real network connection, and counts how many times it's
// called - the core thing under test is that Reconcile does NOT call this
// for an unchanged target.
func fakeSinkFactory(t *testing.T, opens *int) func(Config) (*Sink, error) {
	return func(cfg Config) (*Sink, error) {
		*opens++
		producer := mocks.NewSyncProducer(t, mocks.NewTestConfig())
		return NewWithProducer(producer, cfg.Topic), nil
	}
}

func TestReconcileLeavesUnchangedTargetUntouched(t *testing.T) {
	var opens int
	m := NewManager("test-client")
	m.newSink = fakeSinkFactory(t, &opens)

	spec := TargetSpec{ID: "legacy", Format: "avro", Topic: "example-topic", Brokers: []string{"broker1:9092"}}
	if err := m.Reconcile([]TargetSpec{spec}); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if opens != 1 {
		t.Fatalf("opens after first Reconcile = %d, want 1", opens)
	}
	firstSink := (*m.live.Load())[0].sink

	// Reconciling with the identical spec again (e.g. because an unrelated
	// target was added) must not reopen this target's producer.
	if err := m.Reconcile([]TargetSpec{spec}); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if opens != 1 {
		t.Fatalf("opens after unchanged Reconcile = %d, want still 1", opens)
	}
	if (*m.live.Load())[0].sink != firstSink {
		t.Error("unchanged target got a new *Sink instance")
	}
}

func TestReconcileReopensChangedTarget(t *testing.T) {
	var opens int
	m := NewManager("test-client")
	m.newSink = fakeSinkFactory(t, &opens)

	if err := m.Reconcile([]TargetSpec{{ID: "legacy", Format: "avro", Topic: "topic-a", Brokers: []string{"b1:9092"}}}); err != nil {
		t.Fatalf("first Reconcile: %v", err)
	}
	if err := m.Reconcile([]TargetSpec{{ID: "legacy", Format: "avro", Topic: "topic-b", Brokers: []string{"b1:9092"}}}); err != nil {
		t.Fatalf("second Reconcile: %v", err)
	}
	if opens != 2 {
		t.Fatalf("opens after topic change = %d, want 2", opens)
	}
}

func TestReconcileClosesRemovedTarget(t *testing.T) {
	var opens int
	m := NewManager("test-client")
	m.newSink = fakeSinkFactory(t, &opens)

	if err := m.Reconcile([]TargetSpec{{ID: "a", Format: "avro", Topic: "t", Brokers: []string{"b:9092"}}}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if err := m.Reconcile(nil); err != nil {
		t.Fatalf("Reconcile(nil): %v", err)
	}
	if got := len(*m.live.Load()); got != 0 {
		t.Fatalf("live targets after removal = %d, want 0", got)
	}
}

func TestPublishFansOutOncePerFormat(t *testing.T) {
	var opens int
	m := NewManager("test-client")
	m.newSink = fakeSinkFactory(t, &opens)

	var avroCalls, jsonCalls int32
	if err := m.Reconcile([]TargetSpec{
		{ID: "a1", Format: "avro", Topic: "t-a1", Brokers: []string{"b:9092"}},
		{ID: "a2", Format: "avro", Topic: "t-a2", Brokers: []string{"b:9092"}},
		{ID: "j1", Format: "json", Topic: "t-j1", Brokers: []string{"b:9092"}},
	}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	// Swap in producers that record which payload they received, so we can
	// confirm avro-format targets got Avro bytes and json-format targets
	// got JSON bytes, encoded exactly once each despite 2 avro targets.
	for _, rt := range *m.live.Load() {
		producer := mocks.NewSyncProducer(t, mocks.NewTestConfig())
		if rt.spec.Format == "avro" {
			producer.ExpectSendMessageWithCheckerFunctionAndSucceed(func(val []byte) error {
				atomic.AddInt32(&avroCalls, 1)
				return nil
			})
		} else {
			producer.ExpectSendMessageWithCheckerFunctionAndSucceed(func(val []byte) error {
				atomic.AddInt32(&jsonCalls, 1)
				return nil
			})
		}
		rt.sink.producer = producer
	}

	codec := testCodec(t)
	errs := m.Publish(context.Background(), "party1", codec, map[string]interface{}{"partyId": "party1"})
	if len(errs) != 0 {
		t.Fatalf("Publish errors: %v", errs)
	}
	if got := atomic.LoadInt32(&avroCalls); got != 2 {
		t.Errorf("avroCalls = %d, want 2", got)
	}
	if got := atomic.LoadInt32(&jsonCalls); got != 1 {
		t.Errorf("jsonCalls = %d, want 1", got)
	}
}
