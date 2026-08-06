package kafkasink

import (
	"context"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

func TestSendPublishesKeyedNakedRecord(t *testing.T) {
	cfg := mocks.NewTestConfig()
	cfg.Producer.RequiredAcks = sarama.WaitForLocal

	producer := mocks.NewSyncProducer(t, cfg)
	producer.ExpectSendMessageWithCheckerFunctionAndSucceed(func(val []byte) error {
		if string(val) != "avro-bytes" {
			t.Errorf("published value = %q, want avro-bytes", val)
		}
		return nil
	})

	sink := NewWithProducer(producer, "test-topic")
	defer sink.Close()

	if err := sink.Send(context.Background(), "0:1:party", []byte("avro-bytes")); err != nil {
		t.Fatalf("Send: %v", err)
	}
}

func TestSendPropagatesProducerError(t *testing.T) {
	cfg := mocks.NewTestConfig()
	producer := mocks.NewSyncProducer(t, cfg)
	producer.ExpectSendMessageAndFail(sarama.ErrOutOfBrokers)

	sink := NewWithProducer(producer, "test-topic")
	sink.maxAttempts = 1 // this test is about immediate propagation, not retry behavior
	defer sink.Close()

	if err := sink.Send(context.Background(), "0:1:party", []byte("x")); err == nil {
		t.Error("expected an error when the producer fails")
	}
}

func TestSendRetriesRetriableErrorThenSucceeds(t *testing.T) {
	cfg := mocks.NewTestConfig()
	producer := mocks.NewSyncProducer(t, cfg)
	// A leader election is a textbook retriable condition - the retry loop
	// should ride it out and still deliver the message.
	producer.ExpectSendMessageAndFail(sarama.ErrLeaderNotAvailable)
	producer.ExpectSendMessageWithCheckerFunctionAndSucceed(func(val []byte) error {
		if string(val) != "avro-bytes" {
			t.Errorf("published value = %q, want avro-bytes", val)
		}
		return nil
	})

	sink := NewWithProducer(producer, "test-topic")
	sink.retryBackoff = time.Millisecond
	defer sink.Close()

	if err := sink.Send(context.Background(), "0:1:party", []byte("avro-bytes")); err != nil {
		t.Fatalf("Send: %v, want the retry to succeed on the second attempt", err)
	}
}

func TestSendGivesUpAfterMaxAttempts(t *testing.T) {
	cfg := mocks.NewTestConfig()
	producer := mocks.NewSyncProducer(t, cfg)
	for i := 0; i < 3; i++ {
		producer.ExpectSendMessageAndFail(sarama.ErrLeaderNotAvailable)
	}

	sink := NewWithProducer(producer, "test-topic")
	sink.retryBackoff = time.Millisecond
	sink.maxAttempts = 3
	defer sink.Close()

	if err := sink.Send(context.Background(), "0:1:party", []byte("x")); err == nil {
		t.Error("expected an error once maxAttempts is exhausted")
	}
	// mocks.SyncProducer fails the test itself (via t.Fatal) if Send calls
	// it more or fewer times than expected, so reaching here with exactly
	// 3 queued expectations confirms Send stopped at maxAttempts and did
	// not retry indefinitely.
}

func TestSendDoesNotRetryNonRetriableError(t *testing.T) {
	cfg := mocks.NewTestConfig()
	producer := mocks.NewSyncProducer(t, cfg)
	// Corrupt message is a data problem, not a transient broker condition -
	// retrying it would just waste the retry budget on something that will
	// never succeed.
	producer.ExpectSendMessageAndFail(sarama.ErrInvalidMessage)

	sink := NewWithProducer(producer, "test-topic")
	sink.retryBackoff = time.Millisecond
	defer sink.Close()

	if err := sink.Send(context.Background(), "0:1:party", []byte("x")); err == nil {
		t.Error("expected an error")
	}
	// Only one expectation was queued above - if Send retried a
	// non-retriable error, the mock would fail the test on the second call.
}

func TestNewSaramaConfigMatchesProductionSettings(t *testing.T) {
	cfg := NewSaramaConfig("divolte.collector")
	if cfg.ClientID != "divolte.collector" {
		t.Errorf("ClientID = %q", cfg.ClientID)
	}
	if cfg.Producer.RequiredAcks != sarama.WaitForLocal {
		t.Errorf("RequiredAcks = %v, want WaitForLocal (acks=1)", cfg.Producer.RequiredAcks)
	}
	if cfg.Producer.Retry.Max != 0 {
		t.Errorf("Retry.Max = %d, want 0", cfg.Producer.Retry.Max)
	}
	if cfg.Producer.Compression != sarama.CompressionLZ4 {
		t.Errorf("Compression = %v, want lz4", cfg.Producer.Compression)
	}
	if cfg.Net.MaxOpenRequests != 1 {
		t.Errorf("MaxOpenRequests = %d, want 1", cfg.Net.MaxOpenRequests)
	}
}

func TestNewRejectsMissingConfig(t *testing.T) {
	if _, err := New(Config{Topic: "t"}); err == nil {
		t.Error("expected error with no brokers")
	}
	if _, err := New(Config{Brokers: []string{"localhost:9092"}}); err == nil {
		t.Error("expected error with no topic")
	}
}
