// Command smoketest fires one realistic event-beacon request at a running
// divolte-collector (Go) instance, then consumes the resulting message back
// off the configured Kafka topic and decodes it, to manually verify the
// whole pipeline end-to-end against a live server. Not part of the
// production binary - a dev-only verification tool.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/IBM/sarama"

	"github.com/example/divolte-rewrite/internal/avroenc"
	"github.com/example/divolte-rewrite/internal/event"
	"github.com/example/divolte-rewrite/internal/mincode"
)

func main() {
	serverURL := flag.String("server", "http://localhost:18290", "base URL of the running divolte-collector")
	brokers := flag.String("brokers", "localhost:9092", "comma-separated Kafka brokers")
	topic := flag.String("topic", "divolte_example_event_test", "Kafka topic to consume from (must be the scratch test topic)")
	schemaFile := flag.String("schema", "configs/example/schema.avsc", "path to the .avsc schema")
	flag.Parse()

	codec, err := avroenc.LoadSchemaFile(*schemaFile)
	if err != nil {
		log.Fatalf("loading schema: %v", err)
	}

	consumer, err := sarama.NewConsumer(strings.Split(*brokers, ","), sarama.NewConfig())
	if err != nil {
		log.Fatalf("connecting consumer: %v", err)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(*topic)
	if err != nil {
		log.Fatalf("listing partitions for %s: %v", *topic, err)
	}
	if len(partitions) == 0 {
		log.Fatalf("topic %s has no partitions", *topic)
	}

	pc, err := consumer.ConsumePartition(*topic, partitions[0], sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("consuming partition: %v", err)
	}
	defer pc.Close()

	party := event.DivolteIdentifier{TimestampMillis: time.Now().UnixMilli(), ID: "smoketest-party"}
	session := event.DivolteIdentifier{TimestampMillis: time.Now().UnixMilli(), ID: "smoketest-session"}

	fields := map[string]string{
		"p": party.String(), "s": session.String(), "n": "t", "f": "t",
		"e": fmt.Sprintf("evt-%d", time.Now().UnixNano()),
		"c": event.FormatBase36(time.Now().UnixMilli()),
		"v": "smoketest-pageview",
		"t": "pageView",
		"l": "https://example.com/product/123",
		"w": event.FormatBase36(1920), "h": event.FormatBase36(1080),
		"i": event.FormatBase36(1920), "j": event.FormatBase36(1080), "k": event.FormatBase36(1),
	}
	values := url.Values{}
	for k, v := range fields {
		values.Set(k, v)
	}
	customParams := map[string]interface{}{
		"label": "smoketest-label",
	}

	encodedParams, err := mincode.Encode(customParams)
	if err != nil {
		log.Fatalf("mincode-encoding custom params: %v", err)
	}
	values.Set("u", encodedParams)

	checksum := event.ComputeChecksum(values)
	values.Set("x", event.FormatBase36(int64(checksum)))

	beaconURL := *serverURL + "/webstats/csc-event?" + values.Encode()
	log.Printf("firing beacon request: %s", beaconURL)

	req, _ := http.NewRequest("GET", beaconURL, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Fatalf("sending beacon request: %v", err)
	}
	resp.Body.Close()
	log.Printf("beacon response status: %d", resp.StatusCode)

	log.Printf("waiting up to 10s for the message to arrive on topic %s...", *topic)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	select {
	case msg := <-pc.Messages():
		log.Printf("received message: key=%s valueLen=%d", string(msg.Key), len(msg.Value))
		decoded, err := codec.DecodeNaked(msg.Value)
		if err != nil {
			log.Fatalf("decoding avro record: %v", err)
		}
		for _, field := range []string{"partyId", "sessionId", "pageViewId", "eventType", "location",
			"referer", "userAgentFamily", "detectedDuplicate", "customLabel"} {
			fmt.Printf("  %-20s = %v\n", field, decoded[field])
		}
	case <-ctx.Done():
		log.Println("TIMED OUT waiting for the message - check the server log")
		os.Exit(1)
	}
}
