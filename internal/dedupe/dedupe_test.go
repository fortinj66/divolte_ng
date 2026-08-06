package dedupe

import "testing"

func TestFirstOccurrenceIsNotADuplicate(t *testing.T) {
	m := New(1000)
	if m.IsProbableDuplicate("party-1", "session-1", "event-1") {
		t.Error("first occurrence flagged as duplicate")
	}
}

func TestRepeatOccurrenceIsFlaggedAsDuplicate(t *testing.T) {
	m := New(1000)
	m.IsProbableDuplicate("party-1", "session-1", "event-1")
	if !m.IsProbableDuplicate("party-1", "session-1", "event-1") {
		t.Error("repeated (partyId, sessionId, eventId) not flagged as duplicate")
	}
}

func TestDifferentEventIsNotFlagged(t *testing.T) {
	m := New(1000)
	m.IsProbableDuplicate("party-1", "session-1", "event-1")
	if m.IsProbableDuplicate("party-1", "session-1", "event-2") {
		t.Error("distinct eventId incorrectly flagged as duplicate")
	}
}

func TestDefaultCapacityUsedForNonPositiveInput(t *testing.T) {
	m := New(0)
	if len(m.slots) != 1_000_000 {
		t.Errorf("len(slots) = %d, want default 1,000,000", len(m.slots))
	}
}

func TestManyDistinctKeysRarelyCollide(t *testing.T) {
	m := New(100_000)
	falsePositives := 0
	for i := 0; i < 10_000; i++ {
		key := string(rune(i))
		if m.IsProbableDuplicate("party", "session", key+"-unique") {
			falsePositives++
		}
	}
	// With 100k slots and 10k distinct keys, false positives should be rare
	// (this is inherently probabilistic - a generous threshold avoids test
	// flakiness while still catching a badly broken hash/slot function).
	if falsePositives > 500 {
		t.Errorf("unexpectedly high false-positive rate: %d/10000", falsePositives)
	}
}
