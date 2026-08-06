package adminui

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// primitiveTypes are the Avro primitive types the friendly form offers -
// covers every shape actually used by the real production schema (string,
// boolean, long, int, double) plus float/bytes for completeness.
var primitiveTypes = []string{"string", "int", "long", "float", "double", "boolean", "bytes"}

// arrayItemTypes excludes "bytes" (not meaningfully useful as a list item
// type here, and not used anywhere in the real schema) - the options
// offered for "list item type" in the form.
var arrayItemTypes = []string{"string", "int", "long", "float", "double", "boolean"}

func isPrimitiveType(t string) bool {
	for _, p := range primitiveTypes {
		if p == t {
			return true
		}
	}
	return false
}

// friendlyType is a human-editable decomposition of an Avro field's
// type+default, covering every shape the real production schema uses:
// a bare primitive, an array of a primitive, and either of those wrapped
// nullable with a default of either `null` or a literal value (the
// "non-null-first" union order, e.g. ["int","null"] default 0, matching
// the real schema's "quantity" field).
//
// Anything outside those shapes (records, enums, maps, fixed, multi-branch
// unions, non-primitive array items) falls back to raw-JSON "advanced"
// mode, which simply passes TypeJSON/DefaultJSON through verbatim.
type friendlyType struct {
	BaseType    string // one of primitiveTypes
	IsArray     bool
	ItemType    string // one of primitiveTypes, meaningful only if IsArray
	IsNullable  bool
	DefaultMode string // "none" | "null" | "value" - meaningful only if IsNullable
	DefaultText string // human-entered default, meaningful only if DefaultMode == "value"

	UseAdvanced     bool // true if the parsed type didn't match a friendly shape, OR the user explicitly filled in the advanced fields
	AdvancedType    string
	AdvancedDefault string
}

// parseFriendlyType decomposes a stored field's raw type/default JSON into
// friendly form fields. Falls back to UseAdvanced=true (with the raw JSON
// preserved for display/editing) for any shape it doesn't recognize.
func parseFriendlyType(typeJSON string, hasDefault bool, defaultJSON string) friendlyType {
	advanced := friendlyType{
		BaseType: "string", ItemType: "string", DefaultMode: "none",
		UseAdvanced: true, AdvancedType: typeJSON,
	}
	if hasDefault {
		advanced.AdvancedDefault = defaultJSON
	}

	var raw interface{}
	if err := json.Unmarshal([]byte(typeJSON), &raw); err != nil {
		return advanced
	}

	// Bare primitive, e.g. "string" - no default expected in this schema,
	// but tolerate one (rendered as an advanced-only concept: the friendly
	// builder doesn't offer defaults on non-nullable fields).
	if name, ok := raw.(string); ok {
		if !isPrimitiveType(name) {
			return advanced
		}
		if hasDefault {
			return advanced // non-nullable-with-default isn't a friendly shape
		}
		return friendlyType{BaseType: name, ItemType: "string", DefaultMode: "none"}
	}

	// Bare (non-nullable) array of a primitive, e.g. {"type":"array","items":"string"}.
	if obj, ok := raw.(map[string]interface{}); ok {
		item, isArr := arrayItemTypeName(obj)
		if isArr && isPrimitiveType(item) && !hasDefault {
			return friendlyType{BaseType: item, IsArray: true, ItemType: item, DefaultMode: "none"}
		}
		return advanced
	}

	// A 2-element union, one branch "null": either ["null", X] or [X, "null"].
	arr, ok := raw.([]interface{})
	if !ok || len(arr) != 2 {
		return advanced
	}
	nullFirst, other, ok := splitNullableUnion(arr)
	if !ok {
		return advanced
	}

	ft := friendlyType{IsNullable: true}
	switch v := other.(type) {
	case string:
		if !isPrimitiveType(v) {
			return advanced
		}
		ft.BaseType = v
		ft.ItemType = "string"
	case map[string]interface{}:
		item, isArr := arrayItemTypeName(v)
		if !isArr || !isPrimitiveType(item) {
			return advanced
		}
		ft.BaseType = item
		ft.ItemType = item
		ft.IsArray = true
	default:
		return advanced
	}

	if !hasDefault {
		ft.DefaultMode = "none"
		return ft
	}
	if strings.TrimSpace(defaultJSON) == "null" {
		ft.DefaultMode = "null"
		return ft
	}
	if ft.IsArray {
		return advanced // a non-null array default literal isn't a friendly shape
	}
	text, err := decodeScalarForDisplay(ft.BaseType, defaultJSON)
	if err != nil {
		return advanced
	}
	ft.DefaultMode = "value"
	ft.DefaultText = text
	_ = nullFirst // the union order is re-derived from DefaultMode when building, not preserved literally
	return ft
}

// arrayItemTypeName reports whether obj is {"type":"array","items":<string>}
// and, if so, the item type name.
func arrayItemTypeName(obj map[string]interface{}) (string, bool) {
	if t, _ := obj["type"].(string); t != "array" {
		return "", false
	}
	items, ok := obj["items"].(string)
	return items, ok
}

func splitNullableUnion(arr []interface{}) (nullFirst bool, other interface{}, ok bool) {
	first, firstIsNull := arr[0].(string)
	second, secondIsNull := arr[1].(string)
	switch {
	case firstIsNull && first == "null":
		return true, arr[1], true
	case secondIsNull && second == "null":
		return false, arr[0], true
	default:
		return false, nil, false
	}
}

// decodeScalarForDisplay renders a raw JSON scalar as plain text suitable
// for a form input, e.g. `"foo"` -> `foo`, `42` -> `42`.
func decodeScalarForDisplay(baseType, rawJSON string) (string, error) {
	switch baseType {
	case "string", "bytes":
		var s string
		if err := json.Unmarshal([]byte(rawJSON), &s); err != nil {
			return "", err
		}
		return s, nil
	default:
		return strings.TrimSpace(rawJSON), nil
	}
}

// buildTypeAndDefault is the inverse of parseFriendlyType - constructs the
// raw type/default JSON to store, from either the friendly fields or the
// advanced raw-JSON override.
func buildTypeAndDefault(ft friendlyType) (typeJSON string, hasDefault bool, defaultJSON string, err error) {
	if ft.UseAdvanced {
		if strings.TrimSpace(ft.AdvancedType) == "" {
			return "", false, "", fmt.Errorf("advanced type JSON is required")
		}
		// Catch a malformed advanced-mode JSON at save time, with an error
		// pointing at this specific field - otherwise a typo here (missing
		// brace, trailing comma) is accepted silently as "field saved" and
		// only surfaces later at /publish time via a whole-document parse
		// error that doesn't say which of potentially 200+ fields broke it.
		if !json.Valid([]byte(ft.AdvancedType)) {
			return "", false, "", fmt.Errorf("advanced type JSON is not valid JSON: %s", ft.AdvancedType)
		}
		if strings.TrimSpace(ft.AdvancedDefault) != "" {
			if !json.Valid([]byte(ft.AdvancedDefault)) {
				return "", false, "", fmt.Errorf("advanced default JSON is not valid JSON: %s", ft.AdvancedDefault)
			}
			return ft.AdvancedType, true, ft.AdvancedDefault, nil
		}
		return ft.AdvancedType, false, "", nil
	}

	if !isPrimitiveType(ft.BaseType) {
		return "", false, "", fmt.Errorf("unrecognized base type %q", ft.BaseType)
	}
	itemType := ft.ItemType
	if ft.IsArray && !isPrimitiveType(itemType) {
		return "", false, "", fmt.Errorf("unrecognized array item type %q", itemType)
	}

	var innerType string
	if ft.IsArray {
		innerType = fmt.Sprintf(`{"type":"array","items":%s}`, jsonString(itemType))
	} else {
		innerType = jsonString(ft.BaseType)
	}

	if !ft.IsNullable {
		return innerType, false, "", nil
	}

	switch ft.DefaultMode {
	case "value":
		if ft.IsArray {
			return "", false, "", fmt.Errorf("a literal array default isn't supported here - use the advanced (raw JSON) option")
		}
		valJSON, err := encodeScalarFromText(ft.BaseType, ft.DefaultText)
		if err != nil {
			return "", false, "", fmt.Errorf("default value: %w", err)
		}
		return fmt.Sprintf("[%s,\"null\"]", innerType), true, valJSON, nil
	case "null":
		return fmt.Sprintf(`["null",%s]`, innerType), true, "null", nil
	default: // "none"
		return fmt.Sprintf(`["null",%s]`, innerType), false, "", nil
	}
}

func jsonString(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}

func encodeScalarFromText(baseType, text string) (string, error) {
	switch baseType {
	case "string", "bytes":
		b, _ := json.Marshal(text)
		return string(b), nil
	case "int", "long":
		if _, err := strconv.ParseInt(strings.TrimSpace(text), 10, 64); err != nil {
			return "", fmt.Errorf("%q is not a whole number", text)
		}
		return strings.TrimSpace(text), nil
	case "float", "double":
		if _, err := strconv.ParseFloat(strings.TrimSpace(text), 64); err != nil {
			return "", fmt.Errorf("%q is not a number", text)
		}
		return strings.TrimSpace(text), nil
	case "boolean":
		t := strings.TrimSpace(strings.ToLower(text))
		if t != "true" && t != "false" {
			return "", fmt.Errorf("%q is not true or false", text)
		}
		return t, nil
	default:
		return "", fmt.Errorf("unsupported type %q", baseType)
	}
}
