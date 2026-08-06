package adminui

// builtinOption is one selectable entry in the "built-in" mapping source
// dropdown - Value must exactly match a name internal/mapping/builtins.go
// recognizes.
type builtinOption struct {
	Group string
	Value string
	Label string
}

// builtinOptions enumerates every built-in producer internal/mapping
// supports (see internal/mapping/builtins.go's switch statement), with a
// plain-English label so the admin UI doesn't require knowing the
// underlying mapping-engine vocabulary to use it.
var builtinOptions = []builtinOption{
	{"Identity & session", "partyId", "Party ID (long-lived visitor identifier)"},
	{"Identity & session", "sessionId", "Session ID (identifier for this visit)"},
	{"Identity & session", "pageViewId", "Page view ID (identifier for this page load)"},
	{"Identity & session", "eventType", "Event type (e.g. pageView, click - whatever the tag sent)"},
	{"Identity & session", "firstInSession", "Is this the first event in the session? (true/false)"},
	{"Identity & session", "duplicate", "Is this a duplicate event? (true/false)"},
	{"Identity & session", "corrupt", "Did this event fail its integrity check? (true/false)"},

	{"Page & request", "location", "Page URL"},
	{"Page & request", "referer", "Referrer URL (the page the visitor came from)"},
	{"Page & request", "remoteHost", "Visitor's IP address"},
	{"Page & request", "timestamp", "Server timestamp (when the server received the event)"},
	{"Page & request", "viewportPixelWidth", "Browser window width, in pixels"},
	{"Page & request", "viewportPixelHeight", "Browser window height, in pixels"},
	{"Page & request", "screenPixelWidth", "Screen width, in pixels"},
	{"Page & request", "screenPixelHeight", "Screen height, in pixels"},

	{"Browser & device (parsed from User-Agent)", "userAgentString", "Raw User-Agent header (unparsed)"},
	{"Browser & device (parsed from User-Agent)", "userAgent.name", "Browser name"},
	{"Browser & device (parsed from User-Agent)", "userAgent.family", "Browser family"},
	{"Browser & device (parsed from User-Agent)", "userAgent.vendor", "Browser maker"},
	{"Browser & device (parsed from User-Agent)", "userAgent.type", "Browser type (e.g. Browser, Mobile Browser)"},
	{"Browser & device (parsed from User-Agent)", "userAgent.version", "Browser version"},
	{"Browser & device (parsed from User-Agent)", "userAgent.deviceCategory", "Device category (e.g. Personal computer, Smartphone)"},
	{"Browser & device (parsed from User-Agent)", "userAgent.osFamily", "Operating system"},
	{"Browser & device (parsed from User-Agent)", "userAgent.osVersion", "Operating system version"},
	{"Browser & device (parsed from User-Agent)", "userAgent.osVendor", "Operating system maker"},
}

// builtinOptionGroup is one <optgroup>'s worth of options, in declaration
// order - Go templates sort map keys alphabetically when ranging over a
// map, which would reorder these groups, so this pre-groups them instead.
type builtinOptionGroup struct {
	Group   string
	Options []builtinOption
}

func groupedBuiltinOptions() []builtinOptionGroup {
	var groups []builtinOptionGroup
	index := map[string]int{}
	for _, o := range builtinOptions {
		i, ok := index[o.Group]
		if !ok {
			i = len(groups)
			index[o.Group] = i
			groups = append(groups, builtinOptionGroup{Group: o.Group})
		}
		groups[i].Options = append(groups[i].Options, o)
	}
	return groups
}
