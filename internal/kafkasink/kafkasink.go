// Package kafkasink publishes Avro-encoded event records to Kafka, matching
// a typical production Kafka sink config: acks=1, retries=0,
// compression.type=lz4, max.in.flight.requests.per.connection=1,
// mode=naked (raw Avro bytes, no Confluent wire-format header - see
// internal/avroenc).
//
// Deferred, not implemented (legacy has real, working code for all of
// these, but none are used by any real deployment we've needed to
// support so far; build if and when one is actually needed, validated
// against real traffic):
//   - Confluent Kafka sink mode (a 5-byte schema-registry wire header
//     prepended to each record) - production's sink is "naked" everywhere.
//   - Google Cloud Pub/Sub sink.
//   - Google Cloud Storage (GCS) file sink.
package kafkasink

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
)

// Retry/backoff defaults for transient send failures. Production's Kafka
// producer itself is configured with retries=0 (see NewSaramaConfig) -
// legacy's actual resilience to a broker hiccup comes from an
// application-level retry loop on top of that
// (TopicFlusher/KafkaFlusher.sendBatch re-driving on every processing-pool
// heartbeat until success, applying backpressure the whole time), not from
// the Kafka client itself. Sink.Send reproduces that same "ride out a
// transient failure instead of dropping the event" behavior with a bounded
// retry+backoff loop rather than legacy's unbounded heartbeat-driven retry,
// since Go's worker-per-partyId model has no equivalent heartbeat/pause
// signal - defaultMaxRetryDuration is kept comfortably under
// cmd/divolte-collector's 30s shutdown timeout so a stuck retry loop still
// gives up in time during a graceful shutdown.
const (
	defaultRetryBackoff     = 200 * time.Millisecond
	defaultMaxBackoff       = 5 * time.Second
	defaultMaxRetryDuration = 20 * time.Second
)

// Config mirrors the fields actually set in production's `kafka { producer
// = {...} }` block, plus what's needed to construct the client.
type Config struct {
	Brokers  []string
	ClientID string
	Topic    string
}

// NewSaramaConfig builds a sarama.Config matching production's producer
// settings.
func NewSaramaConfig(clientID string) *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.ClientID = clientID
	cfg.Producer.RequiredAcks = sarama.WaitForLocal // acks = 1
	cfg.Producer.Retry.Max = 0                      // retries = 0
	cfg.Producer.Compression = sarama.CompressionLZ4
	cfg.Net.MaxOpenRequests = 1 // max.in.flight.requests.per.connection = 1
	cfg.Producer.Return.Successes = true
	return cfg
}

// Producer is the minimal interface kafkasink needs from sarama.SyncProducer
// - satisfied by both the real client and github.com/IBM/sarama/mocks.SyncProducer.
type Producer interface {
	SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error)
	Close() error
}

// Sink publishes naked Avro-encoded records, keyed by partyId (matching the
// production Kafka sink's partition-affinity key).
type Sink struct {
	producer Producer
	topic    string

	retryBackoff     time.Duration
	maxBackoff       time.Duration
	maxRetryDuration time.Duration
	maxAttempts      int // test-only: 0 means unlimited (bounded only by maxRetryDuration)
}

// New connects a real sarama SyncProducer to the given brokers.
func New(cfg Config) (*Sink, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafkasink: no brokers configured")
	}
	if cfg.Topic == "" {
		return nil, fmt.Errorf("kafkasink: no topic configured")
	}
	producer, err := sarama.NewSyncProducer(cfg.Brokers, NewSaramaConfig(cfg.ClientID))
	if err != nil {
		return nil, fmt.Errorf("kafkasink: connecting to brokers %v: %w", cfg.Brokers, err)
	}
	return newSink(producer, cfg.Topic), nil
}

// NewWithProducer wraps an already-constructed Producer (e.g. a
// mocks.SyncProducer in tests) for the given topic.
func NewWithProducer(producer Producer, topic string) *Sink {
	return newSink(producer, topic)
}

func newSink(producer Producer, topic string) *Sink {
	return &Sink{
		producer:         producer,
		topic:            topic,
		retryBackoff:     defaultRetryBackoff,
		maxBackoff:       defaultMaxBackoff,
		maxRetryDuration: defaultMaxRetryDuration,
	}
}

// Send publishes one naked Avro-encoded record, keyed by partyID. A
// transient (retriable) failure is retried with exponential backoff for up
// to maxRetryDuration before giving up - see the package doc comment for
// why this exists instead of just failing on the first error.
//
// ctx bounds the retry loop's backoff waits: if ctx is cancelled (e.g. the
// caller is shutting down the processing pool), Send returns promptly
// instead of sleeping through the rest of maxRetryDuration - without this,
// Pool.Stop's shutdown deadline was cosmetic for any worker still inside a
// retry backoff, since nothing could interrupt a bare time.Sleep.
func (s *Sink) Send(ctx context.Context, partyID string, avroBytes []byte) error {
	msg := &sarama.ProducerMessage{
		Topic: s.topic,
		Key:   sarama.StringEncoder(partyID),
		Value: sarama.ByteEncoder(avroBytes),
	}

	deadline := time.Now().Add(s.maxRetryDuration)
	backoff := s.retryBackoff
	var lastErr error
	for attempts := 1; ; attempts++ {
		_, _, err := s.producer.SendMessage(msg)
		if err == nil {
			return nil
		}
		lastErr = err
		exhausted := time.Now().After(deadline) || (s.maxAttempts > 0 && attempts >= s.maxAttempts)
		if !isRetriable(err) || exhausted {
			return fmt.Errorf("kafkasink: sending to topic %q: %w", s.topic, lastErr)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("kafkasink: sending to topic %q: giving up, context cancelled after %d attempt(s): %w", s.topic, attempts, lastErr)
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > s.maxBackoff {
			backoff = s.maxBackoff
		}
	}
}

// isRetriable reports whether err represents a transient condition worth
// retrying - matching Kafka's own "retriable" classification for the
// broker-returned error codes a producer send can hit (leader
// election/unavailability, timeouts, under-replication, storage hiccups),
// plus any error sarama returns that ISN'T one of these well-known protocol
// codes, on the assumption that a client-level failure (can't reach any
// broker, connection reset, etc.) is also almost always transient.
func isRetriable(err error) bool {
	var kerr sarama.KError
	if errors.As(err, &kerr) {
		switch kerr {
		case sarama.ErrLeaderNotAvailable,
			sarama.ErrNotLeaderForPartition,
			sarama.ErrRequestTimedOut,
			sarama.ErrNetworkException,
			sarama.ErrNotEnoughReplicas,
			sarama.ErrNotEnoughReplicasAfterAppend,
			sarama.ErrNotController,
			sarama.ErrKafkaStorageError,
			sarama.ErrPreferredLeaderNotAvailable,
			sarama.ErrUnknownTopicOrPartition,
			sarama.ErrThrottlingQuotaExceeded:
			return true
		default:
			return false
		}
	}
	return true
}

// Close shuts down the underlying producer.
func (s *Sink) Close() error {
	return s.producer.Close()
}
