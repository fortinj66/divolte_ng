// Package jsonenc encodes the generic map[string]interface{} produced by
// internal/mapping as plain JSON - a sibling to internal/avroenc, for
// Kafka output targets configured with format "json" instead of "avro".
// Unlike avroenc.Codec.EncodeNaked, Encode does not mutate or take
// ownership of fields - callers needing to also Avro-encode the same map
// afterward (which does mutate it) should call Encode first.
package jsonenc

import (
	"encoding/json"
	"fmt"
)

// Encode marshals fields to JSON. fields is read-only to Encode.
func Encode(fields map[string]interface{}) ([]byte, error) {
	b, err := json.Marshal(fields)
	if err != nil {
		return nil, fmt.Errorf("jsonenc: encoding: %w", err)
	}
	return b, nil
}
