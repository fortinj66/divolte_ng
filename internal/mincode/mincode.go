// Package mincode decodes the compact "mincode" format used by divolte.js to
// encode custom event parameters into the `u` query parameter of the event
// beacon request. It is not JSON: values are tagged with a single leading
// character, strings terminate on an unescaped '!' (escape character '~'),
// and objects/arrays use '(' ')' / 'a' '.' as delimiters instead of braces.
//
// The non-obvious part of the grammar: inside an object, a field's leading
// tag character is consumed as the *field name's* prefix, but is reused as
// the type tag for the *value* that follows the field name. E.g. "dage!16!"
// decodes to a field named "age" (the leading 'd' is not part of the name)
// whose value is the base36 integer "16" (i.e. 42), because 'd' is the
// integer tag. This is confirmed against divolte-collector's own
// MincodeParser.java and its mincode-samples.json test fixtures.
package mincode

import (
	"fmt"
	"math/big"
	"strconv"
)

// Decode parses a mincode-encoded string into a generic Go value:
// map[string]interface{}, []interface{}, string, bool, int64, float64, or nil.
func Decode(s string) (interface{}, error) {
	d := &decoder{data: s}
	tag, ok := d.readByte()
	if !ok {
		return nil, fmt.Errorf("mincode: empty input")
	}
	v, err := d.readValue(tag)
	if err != nil {
		return nil, err
	}
	if d.pos != len(d.data) {
		return nil, fmt.Errorf("mincode: trailing data at offset %d", d.pos)
	}
	return v, nil
}

// maxNestingDepth bounds how deeply nested objects/arrays this decoder will
// follow. The "u" query param this decodes comes straight from the browser
// with no size/shape limit of its own, so without a cap a crafted beacon URL
// could drive unbounded recursion purely from client-supplied bytes.
const maxNestingDepth = 64

type decoder struct {
	data  string
	pos   int
	depth int
}

func (d *decoder) readByte() (byte, bool) {
	if d.pos >= len(d.data) {
		return 0, false
	}
	b := d.data[d.pos]
	d.pos++
	return b, true
}

func (d *decoder) peekByte() (byte, bool) {
	if d.pos >= len(d.data) {
		return 0, false
	}
	return d.data[d.pos], true
}

// readValue interprets tag as a value-type tag and consumes whatever payload
// (if any) that value type requires.
func (d *decoder) readValue(tag byte) (interface{}, error) {
	switch tag {
	case 's':
		return d.readStringBody()
	case 't':
		return true, nil
	case 'f':
		return false, nil
	case 'n':
		return nil, nil
	case 'o':
		return map[string]interface{}{}, nil
	case 'd':
		text, err := d.readTerminated()
		if err != nil {
			return nil, err
		}
		neg := false
		if len(text) > 0 && text[0] == '-' {
			neg = true
			text = text[1:]
		}
		bi, ok := new(big.Int).SetString(text, 36)
		if !ok {
			return nil, fmt.Errorf("mincode: invalid base36 integer %q", text)
		}
		if neg {
			bi.Neg(bi)
		}
		return bi.Int64(), nil
	case 'j':
		text, err := d.readTerminated()
		if err != nil {
			return nil, err
		}
		f, err := strconv.ParseFloat(text, 64)
		if err != nil {
			return nil, fmt.Errorf("mincode: invalid number %q: %w", text, err)
		}
		return f, nil
	case '(':
		return d.readObject()
	case 'a':
		return d.readArray()
	default:
		return nil, fmt.Errorf("mincode: unknown type tag %q at offset %d", tag, d.pos-1)
	}
}

// readStringBody scans a string/field-name body: characters are copied
// verbatim, '~' escapes the following single character, and an unescaped
// '!' terminates the string (and is consumed).
func (d *decoder) readStringBody() (string, error) {
	var buf []byte
	for {
		b, ok := d.readByte()
		if !ok {
			return "", fmt.Errorf("mincode: unterminated string")
		}
		switch b {
		case '!':
			return string(buf), nil
		case '~':
			esc, ok := d.readByte()
			if !ok {
				return "", fmt.Errorf("mincode: dangling escape at end of input")
			}
			buf = append(buf, esc)
		default:
			buf = append(buf, b)
		}
	}
}

// readTerminated scans raw payload chars (digits, sign, '.', 'e'/'E') up to
// an unescaped '!', used for the 'd' and 'j' numeric encodings. These never
// contain '~' or need escaping, so this is simpler than readStringBody, but
// share the same terminator convention.
func (d *decoder) readTerminated() (string, error) {
	start := d.pos
	for {
		b, ok := d.readByte()
		if !ok {
			return "", fmt.Errorf("mincode: unterminated numeric literal")
		}
		if b == '!' {
			return d.data[start : d.pos-1], nil
		}
	}
}

func (d *decoder) readObject() (map[string]interface{}, error) {
	d.depth++
	defer func() { d.depth-- }()
	if d.depth > maxNestingDepth {
		return nil, fmt.Errorf("mincode: nesting depth exceeds %d", maxNestingDepth)
	}
	obj := map[string]interface{}{}
	for {
		next, ok := d.peekByte()
		if !ok {
			return nil, fmt.Errorf("mincode: unterminated object")
		}
		if next == ')' {
			d.pos++
			return obj, nil
		}
		fieldTag, _ := d.readByte()
		name, err := d.readStringBody()
		if err != nil {
			return nil, fmt.Errorf("mincode: reading field name: %w", err)
		}
		val, err := d.readValue(fieldTag)
		if err != nil {
			return nil, fmt.Errorf("mincode: reading value for field %q: %w", name, err)
		}
		obj[name] = val
	}
}

func (d *decoder) readArray() ([]interface{}, error) {
	d.depth++
	defer func() { d.depth-- }()
	if d.depth > maxNestingDepth {
		return nil, fmt.Errorf("mincode: nesting depth exceeds %d", maxNestingDepth)
	}
	arr := []interface{}{}
	for {
		next, ok := d.peekByte()
		if !ok {
			return nil, fmt.Errorf("mincode: unterminated array")
		}
		if next == '.' {
			d.pos++
			return arr, nil
		}
		elemTag, _ := d.readByte()
		val, err := d.readValue(elemTag)
		if err != nil {
			return nil, fmt.Errorf("mincode: reading array element: %w", err)
		}
		arr = append(arr, val)
	}
}
