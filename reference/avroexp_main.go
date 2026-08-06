package main

import (
	"fmt"

	"github.com/hamba/avro/v2"
)

const schemaJSON = `{
  "type": "record", "name": "t",
  "fields": [
    { "name": "sku_id", "type": ["null", {"type":"array","items":"string"}], "default": null }
  ]
}`

func main() {
	s := avro.MustParse(schemaJSON)

	tryEncode := func(label string, v interface{}) {
		fields := map[string]interface{}{"sku_id": v}
		b, err := avro.Marshal(s, fields)
		fmt.Printf("%s: err=%v bytes=%v\n", label, err, b)
	}

	tryEncode("raw []string", []string{"a", "b"})
	tryEncode("wrapped array key", map[string]interface{}{"array": []string{"a", "b"}})
	tryEncode("nil", nil)
}
