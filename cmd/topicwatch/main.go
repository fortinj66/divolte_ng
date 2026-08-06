// Command topicwatch is a dev tool for inspecting divolte event topics: it
// consumes raw Kafka messages, decodes them as naked Avro against a given
// .avsc schema, and either prints them live (-mode=tail) or aggregates
// field-level statistics over a sampling window (-mode=stats), including a
// side-by-side comparison of two topics at once (-mode=compare) - built to
// compare the legacy Java pipeline's output topic against the Go rewrite's
// scratch topic without needing any Kafka CLI tooling on the boxes.
//
// It only ever consumes individual partitions directly (sarama's low-level
// PartitionConsumer, no consumer group), so it never joins or perturbs any
// consumer group's offsets - safe to point at a live production topic that
// other systems are already consuming.
package main

import (
	"flag"
	"fmt"
	"log"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/example/divolte-rewrite/internal/avroenc"
)

func main() {
	mode := flag.String("mode", "tail", "tail | stats | compare")
	brokers := flag.String("brokers", "kafka-01.example.com:9092,kafka-02.example.com:9092", "comma-separated Kafka brokers")
	topic := flag.String("topic", "", "Kafka topic to read (required)")
	schemaFile := flag.String("schema", "configs/example/schema.avsc", "path to the .avsc schema for -topic")
	topic2 := flag.String("topic2", "", "second topic, required for -mode=compare")
	schema2File := flag.String("schema2", "", "schema for -topic2 (defaults to -schema, since legacy and Go rewrite share the same schema)")
	from := flag.String("from", "newest", "newest | oldest - where to start reading each partition")
	count := flag.Int("count", 0, "stop sampling a topic after N messages (0 = no limit, rely on -duration)")
	duration := flag.Duration("duration", 30*time.Second, "how long to sample for -mode=stats/compare (ignored by -mode=tail unless -count is also 0, in which case tail runs until Ctrl+C)")
	fieldsFlag := flag.String("fields", "partyId,sessionId,pageViewId,eventType,location,userAgentFamily,userAgentOsFamily,detectedDuplicate,detectedCorruption,environment_site", "comma-separated field names to print per record in -mode=tail")
	categoricalFlag := flag.String("categorical", "eventType,userAgentFamily,userAgentOsFamily,detectedDuplicate,detectedCorruption,environment_site", "comma-separated fields to show a value-distribution for in -mode=stats/compare")
	topN := flag.Int("topn", 8, "how many distinct values to show per categorical field")
	flag.Parse()

	if *topic == "" {
		log.Fatal("-topic is required")
	}

	codec, err := avroenc.LoadSchemaFile(*schemaFile)
	if err != nil {
		log.Fatalf("loading schema %s: %v", *schemaFile, err)
	}

	brokerList := strings.Split(*brokers, ",")
	categorical := splitNonEmpty(*categoricalFlag)

	switch *mode {
	case "tail":
		fields := splitNonEmpty(*fieldsFlag)
		runTail(brokerList, *topic, codec, *from, *count, fields)

	case "stats":
		st := sample(brokerList, *topic, codec, *from, *count, *duration, categorical)
		printStats(st, categorical, *topN)

	case "compare":
		if *topic2 == "" {
			log.Fatal("-mode=compare requires -topic2")
		}
		s2file := *schema2File
		if s2file == "" {
			s2file = *schemaFile
		}
		codec2, err := avroenc.LoadSchemaFile(s2file)
		if err != nil {
			log.Fatalf("loading schema %s: %v", s2file, err)
		}

		var wg sync.WaitGroup
		var stA, stB *topicStats
		wg.Add(2)
		go func() {
			defer wg.Done()
			stA = sample(brokerList, *topic, codec, *from, *count, *duration, categorical)
		}()
		go func() {
			defer wg.Done()
			stB = sample(brokerList, *topic2, codec2, *from, *count, *duration, categorical)
		}()
		wg.Wait()
		printComparison(stA, stB, categorical, *topN)

	default:
		log.Fatalf("unknown -mode %q (want tail, stats, or compare)", *mode)
	}
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// runTail prints each decoded record as it arrives, until -count is hit (if
// nonzero) or the process is interrupted.
func runTail(brokers []string, topic string, codec *avroenc.Codec, from string, count int, fields []string) {
	consumer, pcs, err := openPartitions(brokers, topic, from)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	stop := make(chan struct{})
	defer close(stop)
	msgs := merge(pcs, stop)
	n := 0
	for msg := range msgs {
		decoded, err := codec.DecodeNaked(msg.Value)
		if err != nil {
			fmt.Printf("[%s] partition=%d offset=%d DECODE ERROR: %v\n", topic, msg.Partition, msg.Offset, err)
			continue
		}
		n++
		fmt.Printf("[%s] #%d partition=%d offset=%d\n", topic, n, msg.Partition, msg.Offset)
		for _, f := range fields {
			fmt.Printf("    %-20s = %v\n", f, decoded[f])
		}
		if count > 0 && n >= count {
			break
		}
	}
}

type topicStats struct {
	Topic       string
	Count       int
	Fields      map[string]bool // every field name seen in any decoded record, regardless of value - so an always-null field still shows up at 0%
	NonNull     map[string]int
	Categorical map[string]map[string]int
}

// sample consumes from every partition of topic until -duration elapses or
// -count messages have been decoded (whichever comes first), and returns
// per-field non-null counts plus value distributions for the requested
// categorical fields.
func sample(brokers []string, topic string, codec *avroenc.Codec, from string, count int, duration time.Duration, categorical []string) *topicStats {
	consumer, pcs, err := openPartitions(brokers, topic, from)
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	st := &topicStats{
		Topic:       topic,
		Fields:      map[string]bool{},
		NonNull:     map[string]int{},
		Categorical: map[string]map[string]int{},
	}
	for _, f := range categorical {
		st.Categorical[f] = map[string]int{}
	}

	stop := make(chan struct{})
	defer close(stop)
	msgs := merge(pcs, stop)
	deadline := time.After(duration)
	for {
		select {
		case msg, ok := <-msgs:
			if !ok {
				return st
			}
			decoded, err := codec.DecodeNaked(msg.Value)
			if err != nil {
				continue
			}
			st.Count++
			for field, val := range decoded {
				st.Fields[field] = true
				if val != nil {
					st.NonNull[field]++
				}
			}
			for _, f := range categorical {
				st.Categorical[f][fmt.Sprintf("%v", decoded[f])]++
			}
			if count > 0 && st.Count >= count {
				return st
			}
		case <-deadline:
			return st
		}
	}
}

func openPartitions(brokers []string, topic string, from string) (sarama.Consumer, []sarama.PartitionConsumer, error) {
	consumer, err := sarama.NewConsumer(brokers, sarama.NewConfig())
	if err != nil {
		return nil, nil, fmt.Errorf("connecting to %v: %w", brokers, err)
	}
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		consumer.Close()
		return nil, nil, fmt.Errorf("listing partitions for %s: %w", topic, err)
	}
	offset := sarama.OffsetNewest
	if from == "oldest" {
		offset = sarama.OffsetOldest
	}
	var pcs []sarama.PartitionConsumer
	for _, p := range partitions {
		pc, err := consumer.ConsumePartition(topic, p, offset)
		if err != nil {
			consumer.Close()
			return nil, nil, fmt.Errorf("consuming %s/%d: %w", topic, p, err)
		}
		pcs = append(pcs, pc)
	}
	return consumer, pcs, nil
}

// merge fans multiple partition consumers into one channel. stop lets a
// caller that exits early (e.g. runTail's -count limit, or sample's
// count/deadline) signal the forwarder goroutines to give up a blocked send
// for a message they've already pulled off their partition, rather than
// leaking them - each forwarder would otherwise block forever on `out <-
// msg` once nothing is reading from out anymore.
func merge(pcs []sarama.PartitionConsumer, stop <-chan struct{}) <-chan *sarama.ConsumerMessage {
	out := make(chan *sarama.ConsumerMessage)
	var wg sync.WaitGroup
	wg.Add(len(pcs))
	for _, pc := range pcs {
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for msg := range pc.Messages() {
				select {
				case out <- msg:
				case <-stop:
					return
				}
			}
		}(pc)
	}
	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}

func printStats(st *topicStats, categorical []string, topN int) {
	fmt.Printf("=== %s: %d messages sampled ===\n", st.Topic, st.Count)
	if st.Count == 0 {
		fmt.Println("  (no messages arrived in the sampling window)")
		return
	}
	fields := make([]string, 0, len(st.Fields))
	for f := range st.Fields {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	fmt.Println("  field non-null rates:")
	for _, f := range fields {
		pct := 100 * float64(st.NonNull[f]) / float64(st.Count)
		fmt.Printf("    %-32s %6.1f%%  (%d/%d)\n", f, pct, st.NonNull[f], st.Count)
	}
	fmt.Println("  categorical distributions:")
	for _, f := range categorical {
		fmt.Printf("    %s:\n", f)
		for _, vc := range topValues(st.Categorical[f], topN) {
			fmt.Printf("      %-30s %d\n", vc.value, vc.count)
		}
	}
}

func printComparison(a, b *topicStats, categorical []string, topN int) {
	fmt.Printf("=== compare: %s (%d msgs) vs %s (%d msgs) ===\n", a.Topic, a.Count, b.Topic, b.Count)
	if a.Count == 0 || b.Count == 0 {
		fmt.Println("  one or both topics had no traffic in the sampling window - widen -duration or check -from")
	}

	fieldSet := map[string]bool{}
	for f := range a.Fields {
		fieldSet[f] = true
	}
	for f := range b.Fields {
		fieldSet[f] = true
	}
	fields := make([]string, 0, len(fieldSet))
	for f := range fieldSet {
		fields = append(fields, f)
	}
	sort.Strings(fields)

	fmt.Printf("  %-32s %10s %10s   %s\n", "field", a.Topic, b.Topic, "")
	for _, f := range fields {
		pctA, pctB := 0.0, 0.0
		if a.Count > 0 {
			pctA = 100 * float64(a.NonNull[f]) / float64(a.Count)
		}
		if b.Count > 0 {
			pctB = 100 * float64(b.NonNull[f]) / float64(b.Count)
		}
		flag := ""
		if diff := pctA - pctB; diff > 20 || diff < -20 {
			flag = "  <-- mismatch"
		}
		fmt.Printf("  %-32s %9.1f%% %9.1f%%%s\n", f, pctA, pctB, flag)
	}

	fmt.Println("  categorical distributions:")
	for _, f := range categorical {
		fmt.Printf("    %s:\n", f)
		fmt.Printf("      %s:\n", a.Topic)
		for _, vc := range topValues(a.Categorical[f], topN) {
			fmt.Printf("        %-28s %d\n", vc.value, vc.count)
		}
		fmt.Printf("      %s:\n", b.Topic)
		for _, vc := range topValues(b.Categorical[f], topN) {
			fmt.Printf("        %-28s %d\n", vc.value, vc.count)
		}
	}
}

type valueCount struct {
	value string
	count int
}

func topValues(m map[string]int, n int) []valueCount {
	vcs := make([]valueCount, 0, len(m))
	for v, c := range m {
		vcs = append(vcs, valueCount{v, c})
	}
	sort.Slice(vcs, func(i, j int) bool { return vcs[i].count > vcs[j].count })
	if len(vcs) > n {
		vcs = vcs[:n]
	}
	return vcs
}
