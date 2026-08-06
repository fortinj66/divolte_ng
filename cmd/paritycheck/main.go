// Command paritycheck diffs divolte-collector's legacy (Java) and Go
// rewrite output field-by-field for the SAME underlying input, in one of
// two modes:
//
//   - -mode=replay (default): fires a corpus of real, captured beacon
//     requests at two standalone instances - typically the legacy Java
//     server run in isolation on a scratch port/topic, never the real one,
//     and the Go rewrite. Each request's party/session/pageView/event IDs
//     are rewritten to a request-unique marker (checksum recomputed) so
//     results can be correlated by identity - this makes replay mode safe
//     to run against a Go-rewrite scratch topic that real live traffic is
//     also concurrently writing to.
//
//   - -mode=live: for a cmd/trafficmirror setup, where the SAME real
//     request already reaches both legacy (directly) and the Go rewrite
//     (via the mirror's best-effort copy) - no requests are fired here,
//     this just passively consumes both of their output topics for a
//     window and correlates by the events' own partyId+sessionId+
//     pageViewId+eventType, which is already identical between the two
//     copies of a real mirrored request.
//
// Unlike topicwatch's -mode=compare (which compares aggregate statistics
// across two DIFFERENT populations of traffic), both modes here diff the
// SAME logical event's fields directly - a true parity check, not a
// population comparison.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/example/divolte-rewrite/internal/avroenc"
	"github.com/example/divolte-rewrite/internal/event"
)

func main() {
	mode := flag.String("mode", "replay", "replay (fire a fixture at two standalone instances) | live (passively correlate two topics a trafficmirror setup already feeds identically)")
	fixturePath := flag.String("fixture", "", "path to a fixture file of raw beacon requests, one 'path?querystring' per line (required for -mode=replay)")
	legacyURL := flag.String("legacy", "http://127.0.0.1:18292", "base URL of the isolated legacy shadow instance (-mode=replay only)")
	goURL := flag.String("go", "http://127.0.0.1:8290", "base URL of the Go rewrite instance (-mode=replay only)")
	brokers := flag.String("brokers", "kafka-01.example.com:9092,kafka-02.example.com:9092", "comma-separated Kafka brokers")
	legacyTopic := flag.String("legacy-topic", "divolte_example_event_legacy_shadow", "Kafka topic legacy publishes to")
	goTopic := flag.String("go-topic", "divolte_example_event_go_d02", "Kafka topic the Go rewrite publishes to")
	schemaFile := flag.String("schema", "configs/example/schema.avsc", "path to the .avsc schema (same schema both sides use)")
	allowlist := flag.String("allowlist", "cart_sku,timestamp", "comma-separated fields to exclude from mismatch reporting (known, documented, intentional differences - timestamp is server receipt time and will always differ trivially between two separately-processed copies)")
	drainTimeout := flag.Duration("drain", 20*time.Second, "-mode=replay: how long to keep listening for outstanding Kafka messages after firing all requests")
	liveDuration := flag.Duration("duration", 60*time.Second, "-mode=live: how long to passively sample both topics")
	exampleLimit := flag.Int("examples", 3, "max example mismatches to print per field")
	flag.Parse()

	codec, err := avroenc.LoadSchemaFile(*schemaFile)
	if err != nil {
		log.Fatalf("loading schema: %v", err)
	}
	brokerList := strings.Split(*brokers, ",")
	consumer, err := sarama.NewConsumer(brokerList, sarama.NewConfig())
	if err != nil {
		log.Fatalf("connecting to kafka: %v", err)
	}
	defer consumer.Close()

	allow := splitNonEmpty(*allowlist)

	switch *mode {
	case "live":
		runLive(consumer, *legacyTopic, *goTopic, codec, *liveDuration, allow, *exampleLimit)
	case "replay":
		runReplay(consumer, *fixturePath, *legacyURL, *goURL, *legacyTopic, *goTopic, codec, *drainTimeout, allow, *exampleLimit)
	default:
		log.Fatalf("unknown -mode %q (want replay or live)", *mode)
	}
}

func runReplay(consumer sarama.Consumer, fixturePath, legacyURL, goURL, legacyTopic, goTopic string, codec *avroenc.Codec, drainTimeout time.Duration, allow map[string]bool, exampleLimit int) {
	if fixturePath == "" {
		log.Fatal("-fixture is required for -mode=replay")
	}
	requests, err := loadFixture(fixturePath)
	if err != nil {
		log.Fatalf("loading fixture: %v", err)
	}
	if len(requests) == 0 {
		log.Fatal("fixture is empty")
	}
	log.Printf("loaded %d fixture requests", len(requests))

	legacyResults := newResultStore()
	goResults := newResultStore()

	stopCollectors := make(chan struct{})
	var collectWg sync.WaitGroup
	collectWg.Add(2)
	go collectTopic(consumer, legacyTopic, codec, legacyResults, replayMarkerKey, stopCollectors, &collectWg)
	go collectTopic(consumer, goTopic, codec, goResults, replayMarkerKey, stopCollectors, &collectWg)

	// Give the partition consumers a moment to attach at "newest" before we
	// start firing, so we don't race the first few requests.
	time.Sleep(2 * time.Second)

	// Only IDs that were actually fired at both targets go into ids - a
	// fixture line that fails to rewrite (malformed querystring, etc.) is
	// never sent anywhere, so it must not be counted as a "both dropped"
	// parity result below; that bucket is meant to mean "both servers saw
	// and rejected this event", not "this tool never sent it".
	var ids []string
	var skipped int
	var fireWg sync.WaitGroup
	client := &http.Client{Timeout: 5 * time.Second}
	for i, rawReq := range requests {
		id := fmt.Sprintf("paritycheck-%d-%d", i, time.Now().UnixNano())
		rewritten, err := rewriteRequestIDs(rawReq, id)
		if err != nil {
			log.Printf("request %d: skipping, could not rewrite IDs: %v", i, err)
			skipped++
			continue
		}
		ids = append(ids, id)
		fireWg.Add(1)
		go func(path string) {
			defer fireWg.Done()
			for _, base := range []string{legacyURL, goURL} {
				resp, err := client.Get(base + path)
				if err != nil {
					log.Printf("firing at %s: %v", base, err)
					continue
				}
				resp.Body.Close()
			}
		}(rewritten)
		// Small stagger so we don't open hundreds of sockets at once.
		if i%20 == 19 {
			fireWg.Wait()
		}
	}
	fireWg.Wait()
	log.Printf("fired all %d requests at both targets, draining Kafka for %s...", len(requests), drainTimeout)

	time.Sleep(drainTimeout)
	close(stopCollectors)
	collectWg.Wait()

	if skipped > 0 {
		log.Printf("%d fixture line(s) could not be rewritten and were never fired - excluded from the parity report below", skipped)
	}
	reportReplay(ids, legacyResults, goResults, allow, exampleLimit)
}

// runLive passively correlates two topics that a cmd/trafficmirror setup
// already feeds identical copies of real traffic into - no requests are
// fired, this just listens.
func runLive(consumer sarama.Consumer, legacyTopic, goTopic string, codec *avroenc.Codec, duration time.Duration, allow map[string]bool, exampleLimit int) {
	legacyResults := newResultStore()
	goResults := newResultStore()

	stopCollectors := make(chan struct{})
	var collectWg sync.WaitGroup
	collectWg.Add(2)
	go collectTopic(consumer, legacyTopic, codec, legacyResults, liveCorrelationKey, stopCollectors, &collectWg)
	go collectTopic(consumer, goTopic, codec, goResults, liveCorrelationKey, stopCollectors, &collectWg)

	log.Printf("passively correlating %s vs %s for %s...", legacyTopic, goTopic, duration)
	time.Sleep(duration)
	close(stopCollectors)
	collectWg.Wait()

	reportLive(legacyResults, goResults, allow, exampleLimit)
}

func splitNonEmpty(s string) map[string]bool {
	out := map[string]bool{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p != "" {
			out[p] = true
		}
	}
	return out
}

func loadFixture(path string) ([]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		out = append(out, line)
	}
	return out, nil
}

// rewriteRequestIDs replaces the party/session/pageView/event identifiers in
// a captured "path?querystring" request with a fresh, request-unique
// marker, recomputing the checksum so the rewritten request is still valid
// - everything else (the mincode custom-params blob, location, user agent
// implied by whatever the caller sets, etc.) passes through unchanged.
func rewriteRequestIDs(rawReq string, id string) (string, error) {
	pathPart, queryPart, ok := strings.Cut(rawReq, "?")
	if !ok {
		return "", fmt.Errorf("no querystring in %q", rawReq)
	}
	values, err := url.ParseQuery(queryPart)
	if err != nil {
		return "", fmt.Errorf("parsing querystring: %w", err)
	}

	now := time.Now().UnixMilli()
	party := event.DivolteIdentifier{TimestampMillis: now, ID: id}
	session := event.DivolteIdentifier{TimestampMillis: now, ID: id}
	values.Set("p", party.String())
	values.Set("s", session.String())
	values.Set("v", id+"-pv")
	values.Set("e", fmt.Sprintf("evt-%s-%d", id, time.Now().UnixNano()))

	checksum := event.ComputeChecksum(values)
	values.Set("x", event.FormatBase36(int64(checksum)))

	return pathPart + "?" + values.Encode(), nil
}

// resultStore holds a FIFO queue of decoded records per correlation key,
// not just the latest one. -mode=live's key (partyId+sessionId+pageViewId+
// eventType) isn't guaranteed unique - a user can fire several same-type
// events on one pageview (repeated impressions, multiple clicks) - so a
// single-slot last-write-wins store would silently let a later real event
// clobber an earlier one and then pair the wrong two records together,
// reporting a false-positive mismatch. Popping the oldest queued record
// from each side together instead assumes only that both sides receive
// same-keyed events in the same relative order, which holds here since
// they're independently-processed mirrors of the same real traffic.
type resultStore struct {
	mu   sync.Mutex
	byID map[string][]map[string]interface{}
}

func newResultStore() *resultStore {
	return &resultStore{byID: map[string][]map[string]interface{}{}}
}

func (s *resultStore) put(id string, decoded map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.byID[id] = append(s.byID[id], decoded)
}

// pop removes and returns the oldest queued record for id, if any.
func (s *resultStore) pop(id string) (map[string]interface{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	q := s.byID[id]
	if len(q) == 0 {
		return nil, false
	}
	v := q[0]
	s.byID[id] = q[1:]
	return v, true
}

func (s *resultStore) keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.byID))
	for k := range s.byID {
		out = append(out, k)
	}
	return out
}

// count returns the total number of queued records across all keys (not
// the number of distinct keys) - call before draining via pop().
func (s *resultStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for _, q := range s.byID {
		n += len(q)
	}
	return n
}

// replayMarkerKey extracts our injected "paritycheck-<i>-<nanos>" marker
// from a decoded record's partyId (format "0:<base36-timestamp>:<marker>",
// see event.DivolteIdentifier.String()) - used in -mode=replay.
func replayMarkerKey(decoded map[string]interface{}) string {
	partyID, _ := decoded["partyId"].(string)
	if idx := strings.LastIndex(partyID, ":"); idx >= 0 {
		return partyID[idx+1:]
	}
	return ""
}

// liveCorrelationKey identifies a real mirrored event by the combination
// of fields that are identical between legacy's and the Go rewrite's
// independently-processed copies of the same underlying request - used in
// -mode=live, where there's no injected marker to key by.
//
// Deliberately does NOT include eventType: if legacy and Go ever disagreed
// on eventType for the same underlying event, folding it into the key would
// make the two records simply never correlate - each would land in
// onlyLegacy/onlyGo (looking like a dropped event) instead of surfacing as
// an ordinary field mismatch in the report below, which is exactly the bug
// class this tool exists to catch. eventType is compared like any other
// field once two records are matched by this key.
func liveCorrelationKey(decoded map[string]interface{}) string {
	party, _ := decoded["partyId"].(string)
	session, _ := decoded["sessionId"].(string)
	pageView, _ := decoded["pageViewId"].(string)
	if party == "" || session == "" || pageView == "" {
		return ""
	}
	return party + "|" + session + "|" + pageView
}

// collectTopic consumes every partition of topic from the newest offset at
// call time, decodes each message, and stores it keyed by keyFunc(decoded)
// - keys that keyFunc reports as "" (not one of ours, or missing the
// correlation fields) are dropped, so this is safe to run against a topic
// real live traffic is also concurrently writing to.
func collectTopic(consumer sarama.Consumer, topic string, codec *avroenc.Codec, store *resultStore, keyFunc func(map[string]interface{}) string, stop <-chan struct{}, wg *sync.WaitGroup) {
	defer wg.Done()
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		log.Printf("listing partitions for %s: %v", topic, err)
		return
	}
	var pcs []sarama.PartitionConsumer
	for _, p := range partitions {
		pc, err := consumer.ConsumePartition(topic, p, sarama.OffsetNewest)
		if err != nil {
			log.Printf("consuming %s/%d: %v", topic, p, err)
			continue
		}
		pcs = append(pcs, pc)
		defer pc.Close()
	}

	msgs := make(chan *sarama.ConsumerMessage)
	for _, pc := range pcs {
		go func(pc sarama.PartitionConsumer) {
			for msg := range pc.Messages() {
				select {
				case msgs <- msg:
				case <-stop:
					return
				}
			}
		}(pc)
	}

	for {
		select {
		case msg := <-msgs:
			decoded, err := codec.DecodeNaked(msg.Value)
			if err != nil {
				continue
			}
			if key := keyFunc(decoded); key != "" {
				store.put(key, decoded)
			}
		case <-stop:
			return
		}
	}
}

func reportReplay(ids []string, legacy, goResults *resultStore, allow map[string]bool, exampleLimit int) {
	bothPublished, onlyLegacy, onlyGo, bothDropped := 0, 0, 0, 0
	fieldMismatchCount := map[string]int{}
	fieldExamples := map[string][]string{}

	for _, id := range ids {
		l, lok := legacy.pop(id)
		g, gok := goResults.pop(id)

		switch {
		case lok && gok:
			bothPublished++
			for f := range unionFields(l, g) {
				if allow[f] {
					continue
				}
				lv := fmt.Sprintf("%v", l[f])
				gv := fmt.Sprintf("%v", g[f])
				if lv != gv {
					fieldMismatchCount[f]++
					if len(fieldExamples[f]) < exampleLimit {
						fieldExamples[f] = append(fieldExamples[f], fmt.Sprintf("legacy=%q go=%q (id=%s)", lv, gv, id))
					}
				}
			}
		case lok && !gok:
			onlyLegacy++
		case !lok && gok:
			onlyGo++
		default:
			bothDropped++
		}
	}

	fmt.Printf("\n=== parity report: %d requests replayed ===\n", len(ids))
	fmt.Printf("  both published:        %d\n", bothPublished)
	fmt.Printf("  only legacy published: %d  <-- Go rewrite dropped an event legacy kept\n", onlyLegacy)
	fmt.Printf("  only Go published:     %d  <-- Go rewrite kept an event legacy would have dropped\n", onlyGo)
	fmt.Printf("  both dropped:          %d  (e.g. missing required \"namespace\" - expected on both sides)\n", bothDropped)

	if len(fieldMismatchCount) == 0 {
		fmt.Println("\n  no field-level mismatches among requests published by both sides.")
		return
	}

	fields := make([]string, 0, len(fieldMismatchCount))
	for f := range fieldMismatchCount {
		fields = append(fields, f)
	}
	sort.Slice(fields, func(i, j int) bool { return fieldMismatchCount[fields[i]] > fieldMismatchCount[fields[j]] })

	fmt.Printf("\n  field mismatches (out of %d both-published requests):\n", bothPublished)
	for _, f := range fields {
		fmt.Printf("    %-32s %d\n", f, fieldMismatchCount[f])
		for _, ex := range fieldExamples[f] {
			fmt.Printf("        %s\n", ex)
		}
	}
}

// reportLive diffs matched pairs from two topics that a trafficmirror setup
// fed identical copies of real traffic into - correlated by
// liveCorrelationKey rather than a known, pre-fired id list, so there's no
// "both dropped" bucket (we have no ground truth for events neither topic
// captured evidence of).
func reportLive(legacy, goResults *resultStore, allow map[string]bool, exampleLimit int) {
	legacyKeys := legacy.keys()
	goKeys := goResults.keys()
	legacyTotal := legacy.count()
	goTotal := goResults.count()
	seen := make(map[string]bool, len(legacyKeys)+len(goKeys))
	for _, k := range legacyKeys {
		seen[k] = true
	}
	for _, k := range goKeys {
		seen[k] = true
	}

	matched, onlyLegacy, onlyGo := 0, 0, 0
	fieldMismatchCount := map[string]int{}
	fieldExamples := map[string][]string{}

	for k := range seen {
		// Drain both sides' queues for this key together, oldest-first, so
		// a key with multiple real events (e.g. repeated impressions on one
		// pageview) pairs each side's Nth occurrence with the other's Nth
		// occurrence instead of a single-slot store letting a later event
		// silently clobber an earlier one and mis-pair the wrong two
		// records - see resultStore's doc comment.
		for {
			l, lok := legacy.pop(k)
			g, gok := goResults.pop(k)
			if !lok && !gok {
				break
			}
			switch {
			case lok && gok:
				matched++
				for f := range unionFields(l, g) {
					if allow[f] {
						continue
					}
					lv := fmt.Sprintf("%v", l[f])
					gv := fmt.Sprintf("%v", g[f])
					if lv != gv {
						fieldMismatchCount[f]++
						if len(fieldExamples[f]) < exampleLimit {
							fieldExamples[f] = append(fieldExamples[f], fmt.Sprintf("legacy=%q go=%q (key=%s)", lv, gv, k))
						}
					}
				}
			case lok:
				onlyLegacy++
			case gok:
				onlyGo++
			}
		}
	}

	fmt.Printf("\n=== live parity report (real mirrored traffic) ===\n")
	fmt.Printf("  legacy events observed: %d\n", legacyTotal)
	fmt.Printf("  go events observed:     %d\n", goTotal)
	fmt.Printf("  matched (same partyId+sessionId+pageViewId on both, paired oldest-first): %d\n", matched)
	fmt.Printf("  only legacy (mirror missed it, or Go dropped it): %d\n", onlyLegacy)
	fmt.Printf("  only Go (shouldn't normally happen - legacy is the mirror's primary leg): %d\n", onlyGo)

	if len(fieldMismatchCount) == 0 {
		fmt.Println("\n  no field-level mismatches among matched events.")
		return
	}

	fields := make([]string, 0, len(fieldMismatchCount))
	for f := range fieldMismatchCount {
		fields = append(fields, f)
	}
	sort.Slice(fields, func(i, j int) bool { return fieldMismatchCount[fields[i]] > fieldMismatchCount[fields[j]] })

	fmt.Printf("\n  field mismatches (out of %d matched events):\n", matched)
	for _, f := range fields {
		fmt.Printf("    %-32s %d\n", f, fieldMismatchCount[f])
		for _, ex := range fieldExamples[f] {
			fmt.Printf("        %s\n", ex)
		}
	}
}

func unionFields(a, b map[string]interface{}) map[string]bool {
	out := map[string]bool{}
	for f := range a {
		out[f] = true
	}
	for f := range b {
		out[f] = true
	}
	return out
}
