# The tracking tag (`divolte_ng.js`)

This is the client-side half of the pipeline: a small JavaScript file
that runs in the visitor's browser, generates the identifiers, and fires
the beacon requests this server receives. It's a customized build of the
original [Divolte Collector](https://github.com/divolte/divolte-collector)
tag (`assets/divolte_ng.js`, embedded into the server binary via
`//go:embed` as `assets.DivolteNGJS`), served directly by the collector
at `prefix + script_name` (`/webstats/divolte_ng.js` with the defaults in
`configs/server.yaml`).

## Including it on a page

Add one `<script>` tag to every page you want to track - typically via a
shared template/footer, not copy-pasted per page:

```html
<script src="//your-collector-host/webstats/divolte_ng.js" defer async></script>
```

Loading it does three things automatically, with zero other integration
required:

1. Generates (or reads existing cookies for) a **party ID** - a
   long-lived visitor identifier - and a **session ID** - reset after a
   period of inactivity.
2. Generates a **page-view ID** for the current page.
3. Fires a `pageView` event (unless disabled - see "Constants baked into
   the script" below), sending the location, referrer, and screen/
   viewport dimensions along with those identifiers.

## Firing custom events

Once the tag has loaded, `window.divolte` is available on the page.
Fire an event with `divolte.signal(type, customParameters)`:

```javascript
// The first argument is the event type; the second is an arbitrary
// JavaScript object of custom parameters, which may be omitted entirely.
divolte.signal('addToCart', { sku: 'ABC123', quantity: 2 });
```

`type` becomes the beacon's `eventType`. `customParameters` gets encoded
into the beacon's `u` querystring parameter (via the same compact
"mincode" binary encoding `internal/mincode` decodes server-side) and is
exactly what an `event_param`/`event_param_path` mapping rule reads from
- see `internal/mapping` and `configs/example/mapping.yaml`:

```yaml
fields:
  - field: customLabel
    event_param: label   # reads customParameters.label from divolte.signal(...)
```

**Custom parameters always arrive as strings on the server side** (for
safety) - if a mapping rule needs a number, coerce it explicitly:

```yaml
  - field: someIntField
    event_param: someParam
    coerce: int32
```

## Waiting for delivery before navigating away

Events are delivered in the background, in the order signalled. If the
page navigates away immediately after `signal()`, the in-flight request
can be aborted before it reaches the server. `divolte.whenCommitted(callback, timeoutMillis)`
invokes `callback` once every pending event has been delivered, or after
`timeoutMillis`, whichever comes first - useful for instrumenting a link
click without silently dropping the event:

```html
<script>
  function clickThrough(link) {
    var destination = link.getAttribute('href');
    divolte.signal('clickThrough', { destination: destination });
    divolte.whenCommitted(function () {
      window.location = destination;
    }, 1000);
    return false; // cancel the default navigation until whenCommitted fires
  }
</script>
```

## What `window.divolte` exposes

| Property | Meaning |
|---|---|
| `divolte.partyId` | The current visitor's long-lived party ID. |
| `divolte.sessionId` | The current session ID. |
| `divolte.pageViewId` | The current page view's ID. |
| `divolte.isNewPartyId` | `true` if this is this visitor's very first pageview ever seen. |
| `divolte.isFirstInSession` | `true` if this is the first pageview of the current session. |
| `divolte.signal(type, params?)` | Fire a custom event; returns its generated event ID. |
| `divolte.whenCommitted(cb, timeoutMs?)` | Invoke `cb` once all signalled events are delivered (or the timeout elapses). |

## Constants baked into the script (not runtime-configurable)

Unlike the original Java server (which exposed `javascript.name`,
`cookie_domain`, `party_timeout`, `session_timeout`,
`javascript.auto_page_view_event`, `javascript.event_timeout`, and
`javascript.logging` as HOCON config per browser source), **this Go
rewrite only exposes `prefix`/`script_name`/`event_suffix` at runtime**
(`configs/server.yaml`). Everything else is a `@define` constant at the
top of `assets/divolte_ng.js` itself:

```javascript
var PARTY_COOKIE_NAME = '_dvp';
var PARTY_ID_TIMEOUT_SECONDS = 2 * 365 * 24 * 60 * 60;  // 2 years
var SESSION_COOKIE_NAME = '_dvs';
var SESSION_ID_TIMEOUT_SECONDS = 30 * 60;               // 30 minutes
var EVENT_TIMEOUT_SECONDS = 1.0;
var COOKIE_DOMAIN = '';                                  // empty = current page's domain
var LOGGING = false;
var AUTO_PAGE_VIEW_EVENT = false;
```

To change any of these, edit them directly in `assets/divolte_ng.js` and
rebuild - there's no server.yaml equivalent. (`AUTO_PAGE_VIEW_EVENT` is
already `false` here, unlike upstream's default of `true` - if you want
the tag to fire `pageView` automatically on load, flip it to `true` and
rebuild.)
