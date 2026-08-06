package syncplugin

import (
	"errors"
	"strings"
	"testing"
)

type fakePlugin struct {
	name    string
	msg     string
	err     error
	calls   int
	gotJSON string
}

func (f *fakePlugin) Name() string { return f.name }

func (f *fakePlugin) Sync(schemaJSON string, fields []Field) (string, error) {
	f.calls++
	f.gotJSON = schemaJSON
	return f.msg, f.err
}

func TestRunAllReturnsEmptyForNoPlugins(t *testing.T) {
	msg, err := RunAll(nil, "{}", nil)
	if msg != "" || err != nil {
		t.Errorf("RunAll(nil, ...) = (%q, %v), want (\"\", nil)", msg, err)
	}
}

func TestRunAllRunsEveryPluginRegardlessOfEarlierFailure(t *testing.T) {
	p1 := &fakePlugin{name: "A", err: errors.New("boom")}
	p2 := &fakePlugin{name: "B", msg: "did the thing"}
	msg, err := RunAll([]Plugin{p1, p2}, "{}", []Field{{Name: "f1"}})
	if p1.calls != 1 || p2.calls != 1 {
		t.Fatalf("expected both plugins called once, got p1=%d p2=%d", p1.calls, p2.calls)
	}
	if err == nil {
		t.Error("RunAll should return an error when any plugin fails")
	}
	if !strings.Contains(msg, "A FAILED: boom") || !strings.Contains(msg, "B: did the thing") {
		t.Errorf("summary = %q, missing expected per-plugin detail", msg)
	}
}

func TestRunAllSucceedsWhenAllPluginsSucceed(t *testing.T) {
	p1 := &fakePlugin{name: "A", msg: "ok1"}
	p2 := &fakePlugin{name: "B", msg: "ok2"}
	msg, err := RunAll([]Plugin{p1, p2}, "{}", nil)
	if err != nil {
		t.Errorf("RunAll should not error when every plugin succeeds, got: %v", err)
	}
	if !strings.Contains(msg, "A: ok1") || !strings.Contains(msg, "B: ok2") {
		t.Errorf("summary = %q, missing expected content", msg)
	}
}

func TestRunAllPassesSchemaJSONToEachPlugin(t *testing.T) {
	p := &fakePlugin{name: "A"}
	if _, err := RunAll([]Plugin{p}, `{"fields":[]}`, nil); err != nil {
		t.Fatalf("RunAll: %v", err)
	}
	if p.gotJSON != `{"fields":[]}` {
		t.Errorf("plugin received schemaJSON = %q", p.gotJSON)
	}
}
