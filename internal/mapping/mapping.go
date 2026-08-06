// Package mapping implements a declarative replacement for divolte-
// collector's Groovy mapping DSL. A typical production mapping uses only
// a small, fixed set of building blocks - built-in event metadata
// producers, the userAgent() accessor chain, scalar/path lookups into the
// JS tag's custom event parameters, and int32/fp64 coercion, plus
// conditionals (a Default value for an absent field) - so a static YAML
// rule list covers it without needing a general expression engine.
//
// Deferred, not implemented (a full catalog of the legacy DSL against
// docs/mapping_reference.rst confirmed these are real, working legacy
// capabilities with no uses found in real production mappings we've
// needed to support; build the specific one that's actually needed,
// against real mapping content that needs it, rather than speculatively
// rebuilding DSL generality now):
//   - cookie(name) - reading an HTTP cookie value.
//   - header(name) (+ .first()/.last()/.get()/.commaSeparated()) - reading
//     a request header.
//   - Regex matching: match(regex).against(producer) / .matches() / .group().
//   - Digest/hashing: digest(algorithm[, seed]).add(...).result(), with
//     hex/base64 formatting.
//   - ip2geo()/GeoIP subfields - a server-side MaxMind database lookup by
//     IP - legacy's ip2geo feature is real code but has never been
//     configured in any real deployment we've needed to support.
//   - parse(...).to(uri)/.to(bool)/.to(int64), and URI-derived accessors
//     (.path()/.host()/.query() etc.) - only .to(int32)/.to(fp64) exist
//     here, matching the only two coercions the real mapping ever uses.
//   - exit()/stop()/section{} branching beyond a plain Default value.
package mapping

import (
	"fmt"
	"log"
	"math"
	"strconv"
	"strings"

	"github.com/example/divolte-rewrite/internal/event"
	"github.com/example/divolte-rewrite/internal/uaparse"
)

// FieldRule describes how to derive one Avro field's value. Exactly one of
// Builtin, EventParam, or EventParamPath should be set.
type FieldRule struct {
	Field string `yaml:"field"`

	// Builtin references a built-in producer, e.g. "duplicate", "corrupt",
	// "timestamp", "userAgent.family". See builtinProducers.
	Builtin string `yaml:"builtin,omitempty"`

	// EventParam looks up a scalar value by key in the mincode-decoded
	// custom event parameters (equivalent to eventParameters().value(key)).
	// When absent and Coerce is empty, this resolves to "" rather than
	// absent - matching .value(key)'s Jackson-backed
	// JsonNode.path(key).asText() semantics, which never signals "missing".
	EventParam string `yaml:"event_param,omitempty"`

	// EventParamPath looks up a value by a simple dotted path in the custom
	// event parameters (equivalent to eventParameters().path('$.key')) -
	// production only ever uses single-level "$.key" paths, so this is a
	// bare key today, but resolved through a dotted-path walker so a future
	// "$.a.b" mapping would also work without a format change.
	EventParamPath string `yaml:"event_param_path,omitempty"`

	// Coerce applies a type conversion to the resolved value: "int32" or
	// "fp64". Empty means no coercion (value used as-is / stringified).
	Coerce string `yaml:"coerce,omitempty"`

	// Default is a literal fallback substituted when the producer's value
	// is absent (nil/not-present) - used for the production mapping's
	// "referer defaults to empty string" case. A nil Default means the
	// field is simply omitted from the encoded record when absent (Avro
	// will apply the schema's own default, if any).
	Default *string `yaml:"default,omitempty"`
}

// Config is the top-level YAML document shape.
type Config struct {
	Fields []FieldRule `yaml:"fields"`
}

// Context carries everything a rule might need to resolve a value for one
// event. CustomParams is the mincode-decoded "u" param (nil if absent or
// undecodable); UserAgent is parsed once per event from RawUserAgent by the
// caller (or lazily via ContextForEvent).
type Context struct {
	Event        *event.BrowserEvent
	CustomParams map[string]interface{}
	Duplicate    bool
	UserAgent    uaparse.Info
}

// NewContext builds a Context for one event, parsing the User-Agent header
// once (the mapping may reference several userAgent.* accessors per event).
func NewContext(ev *event.BrowserEvent, customParams map[string]interface{}, duplicate bool) Context {
	return Context{
		Event:        ev,
		CustomParams: customParams,
		Duplicate:    duplicate,
		UserAgent:    uaparse.Parse(ev.RawUserAgent),
	}
}

// Evaluate resolves every rule in the config against ctx and returns a
// map[string]interface{} keyed by Avro field name, ready for
// internal/avroenc to encode. Fields whose value is absent (and have no
// Default) are simply omitted from the map - the Avro encoder applies the
// schema's own default (or errors, for a required field with none, matching
// the Java server's behavior when a mapping's data doesn't cover a required
// field).
//
// Rules are applied in declaration order; if two rules target the same
// field, the later rule wins, matching a Groovy-style mapping's
// sequential record-field writes.
func (c *Config) Evaluate(ctx Context) (map[string]interface{}, error) {
	out := make(map[string]interface{}, len(c.Fields))
	for _, rule := range c.Fields {
		val, present, err := resolve(rule, ctx)
		if err != nil {
			return nil, fmt.Errorf("mapping: field %q: %w", rule.Field, err)
		}
		if !present {
			if rule.Default != nil {
				out[rule.Field] = *rule.Default
			}
			continue
		}
		out[rule.Field] = val
	}
	return out, nil
}

func resolve(rule FieldRule, ctx Context) (interface{}, bool, error) {
	var raw interface{}
	var present bool

	switch {
	case rule.Builtin != "":
		v, ok, err := builtinValue(rule.Builtin, ctx)
		if err != nil {
			return nil, false, err
		}
		raw, present = v, ok
	case rule.EventParam != "":
		v, ok := lookupScalar(ctx.CustomParams, rule.EventParam)
		switch {
		case ok:
			// eventParameters().value(key) is a String producer - Jackson's
			// JsonNode.asText() stringifies whatever was found (a number,
			// a boolean, anything), it does not require the source to
			// already be a string. A coercion rule (if any) then re-parses
			// this string back to a number, matching parse(...).to(int32),
			// which itself operates on that same String producer - so
			// stringifying here first is correct for both coerced and
			// uncoerced event_param fields. Found via cmd/paritycheck: real
			// traffic sends metrics_dns_lookup/search_results_page_number
			// etc. as numbers even though the schema types them as string,
			// and legacy publishes them fine (as their string form) while
			// the Go rewrite was failing the whole record.
			raw, present = stringifyLikeJackson(v), true
		case rule.Coerce == "" && ctx.CustomParams != nil:
			// Legacy's eventParameters().value(key) reads the value via
			// Jackson's JsonNode.path(key).asText(), which returns "" for a
			// key missing from an otherwise-present custom-params object
			// (a MissingNode within a real JsonNode). But when the custom-
			// params blob itself is entirely absent (no "u" param on the
			// request at all), eventParameters()'s own Optional is empty
			// and .value(key)'s .map() chain never runs - so the field
			// stays genuinely absent there too, which is why a plain
			// pageview beacon with no custom params still gets dropped by
			// legacy for a required field like namespace. Confirmed against
			// a real legacy instance via cmd/paritycheck - the first pass
			// at this fix wrongly defaulted to "" in both cases.
			raw, present = "", true
		default:
			raw, present = nil, false
		}
	case rule.EventParamPath != "":
		v, ok := lookupPath(ctx.CustomParams, rule.EventParamPath)
		raw, present = v, ok
	default:
		return nil, false, fmt.Errorf("rule has no source (builtin/event_param/event_param_path)")
	}

	if !present {
		return nil, false, nil
	}

	if rule.Coerce != "" {
		coerced, err := coerce(raw, rule.Coerce)
		if err != nil {
			// Real clients occasionally send a value of the wrong shape for
			// a coerced field (e.g. an array where product_quantity expects
			// a scalar). Legacy's equivalent DSL construct (parse(...).to
			// (int32), backed by Mapping.toInt/toLong/etc.) treats a failed
			// parse as "value absent" - the field falls back to its
			// Default/schema default rather than aborting the whole
			// record - so we do the same instead of failing the whole event
			// over one malformed field.
			log.Printf("mapping: field %q: coercing to %s: %v - treating as absent", rule.Field, rule.Coerce, err)
			return nil, false, nil
		}
		return coerced, true, nil
	}
	return raw, true, nil
}

// lookupScalar mirrors eventParameters().value(key): a direct top-level
// lookup returning the raw decoded value (string/number/bool) as-is.
func lookupScalar(params map[string]interface{}, key string) (interface{}, bool) {
	if params == nil {
		return nil, false
	}
	v, ok := params[key]
	if !ok || v == nil {
		return nil, false
	}
	return v, true
}

// lookupPath mirrors eventParameters().path('$.key'): resolved through a
// dotted-path walker. Production mappings only ever use a bare top-level
// key ("$.key" -> just "key"), but this walks nested maps for any future
// "$.a.b" style path too.
func lookupPath(params map[string]interface{}, path string) (interface{}, bool) {
	if params == nil {
		return nil, false
	}
	var cur interface{} = params
	for _, seg := range strings.Split(path, ".") {
		m, ok := cur.(map[string]interface{})
		if !ok {
			return nil, false
		}
		v, ok := m[seg]
		if !ok || v == nil {
			return nil, false
		}
		cur = v
	}
	return cur, true
}

// stringifyLikeJackson mirrors Jackson's JsonNode.asText(), which
// eventParameters().value(key) uses under the hood: it stringifies whatever
// value the node holds rather than requiring it to already be a string.
// Jackson's asText() returns "" for a container node (object/array) rather
// than a Go-style dump of its contents - matched here so a plain EventParam
// that happens to resolve to a nested map/slice degrades to an empty string
// like every other unexpected shape, instead of leaking a Go-internal
// representation (e.g. "map[a:1]") into the encoded record.
func stringifyLikeJackson(v interface{}) string {
	switch n := v.(type) {
	case string:
		return n
	case bool:
		return strconv.FormatBool(n)
	case int64:
		return strconv.FormatInt(n, 10)
	case float64:
		return strconv.FormatFloat(n, 'g', -1, 64)
	case map[string]interface{}, []interface{}:
		return ""
	default:
		return fmt.Sprintf("%v", n)
	}
}

func coerce(v interface{}, kind string) (interface{}, error) {
	switch kind {
	case "int32":
		switch n := v.(type) {
		case int64:
			if n < math.MinInt32 || n > math.MaxInt32 {
				return nil, fmt.Errorf("value %d out of int32 range", n)
			}
			return int32(n), nil
		case float64:
			if math.IsNaN(n) || n < math.MinInt32 || n > math.MaxInt32 {
				return nil, fmt.Errorf("value %v out of int32 range", n)
			}
			return int32(n), nil
		case string:
			i, err := strconv.ParseInt(strings.TrimSpace(n), 10, 32)
			if err != nil {
				return nil, err
			}
			return int32(i), nil
		default:
			return nil, fmt.Errorf("cannot coerce %T to int32", v)
		}
	case "fp64":
		switch n := v.(type) {
		case int64:
			return float64(n), nil
		case float64:
			if math.IsNaN(n) || math.IsInf(n, 0) {
				return nil, fmt.Errorf("value %v is not finite", n)
			}
			return n, nil
		case string:
			f, err := strconv.ParseFloat(strings.TrimSpace(n), 64)
			if err != nil {
				return nil, err
			}
			if math.IsNaN(f) || math.IsInf(f, 0) {
				return nil, fmt.Errorf("value %q is not finite", n)
			}
			return f, nil
		default:
			return nil, fmt.Errorf("cannot coerce %T to fp64", v)
		}
	default:
		return nil, fmt.Errorf("unknown coercion %q", kind)
	}
}
