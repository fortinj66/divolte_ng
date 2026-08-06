package mapping

import "fmt"

// builtinValue resolves one of the fixed built-in producers used by the
// real production mapping. Returns (value, present, error) - present=false
// means "absent", handled the same way as an absent event-param lookup
// (Default substitution or field omission).
func builtinValue(name string, ctx Context) (interface{}, bool, error) {
	ev := ctx.Event

	switch name {
	case "duplicate":
		return ctx.Duplicate, true, nil
	case "corrupt":
		return !ev.ChecksumCorrect, true, nil
	case "firstInSession":
		return ev.IsFirstInSession, true, nil
	case "timestamp":
		return ev.ReceivedAtMillis, true, nil
	case "remoteHost":
		return ev.RemoteHost, true, nil
	case "referer":
		return stringPtrValue(ev.Referer)
	case "location":
		return stringPtrValue(ev.Location)
	case "viewportPixelWidth":
		return intPtrValue(ev.ViewportPixelWidth)
	case "viewportPixelHeight":
		return intPtrValue(ev.ViewportPixelHeight)
	case "screenPixelWidth":
		return intPtrValue(ev.ScreenPixelWidth)
	case "screenPixelHeight":
		return intPtrValue(ev.ScreenPixelHeight)
	case "partyId":
		return ev.PartyID.String(), true, nil
	case "sessionId":
		return ev.SessionID.String(), true, nil
	case "pageViewId":
		return ev.PageViewID, true, nil
	case "eventType":
		return stringPtrValue(ev.EventType)
	case "userAgentString":
		return ev.RawUserAgent, true, nil
	case "userAgent.name":
		return ctx.UserAgent.Name, true, nil
	case "userAgent.family":
		return ctx.UserAgent.Family, true, nil
	case "userAgent.vendor":
		return ctx.UserAgent.Vendor, true, nil
	case "userAgent.type":
		return ctx.UserAgent.Type, true, nil
	case "userAgent.version":
		return ctx.UserAgent.Version, true, nil
	case "userAgent.deviceCategory":
		return ctx.UserAgent.DeviceCategory, true, nil
	case "userAgent.osFamily":
		return ctx.UserAgent.OSFamily, true, nil
	case "userAgent.osVersion":
		return ctx.UserAgent.OSVersion, true, nil
	case "userAgent.osVendor":
		return ctx.UserAgent.OSVendor, true, nil
	default:
		return nil, false, fmt.Errorf("unknown builtin producer %q", name)
	}
}

func stringPtrValue(p *string) (interface{}, bool, error) {
	if p == nil {
		return nil, false, nil
	}
	return *p, true, nil
}

func intPtrValue(p *int) (interface{}, bool, error) {
	if p == nil {
		return nil, false, nil
	}
	return *p, true, nil
}
