// Package assets embeds the static files served by the browser source: the
// tracking tag (divolte_ng.js - a copy of the real divolte.js with this
// source's constants overridden, see the comments in divolte_ng.js) and
// the 1x1 transparent GIF the event beacon responds with.
package assets

import _ "embed"

//go:embed divolte_ng.js
var DivolteNGJS []byte

//go:embed transparent1x1.gif
var Transparent1x1GIF []byte
