// Package uaparse is a lightweight, dependency-free User-Agent classifier.
//
// This is a KNOWN, ACCEPTED non-parity gap versus the original Java server
// (which used net.sf.uadetector, an unmaintained library with no faithful
// Go port and its own large regex database). Field values here will not
// match uadetector's exact taxonomy/output byte-for-byte - see the rewrite
// plan's explicit non-goals. This covers the common desktop/mobile
// browsers and OSes well enough for analytics purposes.
package uaparse

import (
	"regexp"
	"strings"
)

// Info mirrors the fields the original mapping pulls off userAgent():
// name/family/vendor/type/version/deviceCategory/osFamily/osVersion/osVendor.
type Info struct {
	Name           string
	Family         string
	Vendor         string
	Type           string
	Version        string
	DeviceCategory string
	OSFamily       string
	OSVersion      string
	OSVendor       string
}

var (
	reEdge    = regexp.MustCompile(`Edg(?:A|iOS)?/([\d.]+)`)
	reOpera   = regexp.MustCompile(`OPR/([\d.]+)`)
	reChrome  = regexp.MustCompile(`Chrome/([\d.]+)`)
	reFirefox = regexp.MustCompile(`Firefox/([\d.]+)`)
	reIE      = regexp.MustCompile(`MSIE ([\d.]+)|rv:([\d.]+)\) like Gecko`)
	reSafari  = regexp.MustCompile(`Version/([\d.]+).*Safari`)
	reSamsung = regexp.MustCompile(`SamsungBrowser/([\d.]+)`)

	reWindows = regexp.MustCompile(`Windows NT ([\d.]+)`)
	reMac     = regexp.MustCompile(`Mac OS X ([\d_.]+)`)
	reIOS     = regexp.MustCompile(`(?:iPhone|iPad|iPod).*OS ([\d_]+)`)
	reAndroid = regexp.MustCompile(`Android ([\d.]+)`)
	reLinux   = regexp.MustCompile(`Linux`)
)

var windowsVersionNames = map[string]string{
	"10.0": "10", "6.3": "8.1", "6.2": "8", "6.1": "7", "6.0": "Vista", "5.1": "XP",
}

// Parse classifies a raw User-Agent header string. Never fails - an
// unrecognized/empty input just yields zero-valued (empty string) fields,
// matching "unknown" rather than erroring.
func Parse(ua string) Info {
	var info Info

	switch {
	case reEdge.MatchString(ua):
		m := reEdge.FindStringSubmatch(ua)
		info.Name, info.Family, info.Vendor = "Edge", "Edge", "Microsoft Corporation"
		info.Version = m[1]
	case reOpera.MatchString(ua):
		m := reOpera.FindStringSubmatch(ua)
		info.Name, info.Family, info.Vendor = "Opera", "Opera", "Opera Software"
		info.Version = m[1]
	case reSamsung.MatchString(ua):
		m := reSamsung.FindStringSubmatch(ua)
		info.Name, info.Family, info.Vendor = "Samsung Browser", "Samsung Browser", "Samsung"
		info.Version = m[1]
	case reChrome.MatchString(ua):
		m := reChrome.FindStringSubmatch(ua)
		info.Name, info.Family, info.Vendor = "Chrome", "Chrome", "Google Inc."
		info.Version = m[1]
	case reFirefox.MatchString(ua):
		m := reFirefox.FindStringSubmatch(ua)
		info.Name, info.Family, info.Vendor = "Firefox", "Firefox", "Mozilla Foundation"
		info.Version = m[1]
	case reSafari.MatchString(ua):
		m := reSafari.FindStringSubmatch(ua)
		info.Name, info.Family, info.Vendor = "Safari", "Safari", "Apple Inc."
		info.Version = m[1]
	case reIE.MatchString(ua):
		m := reIE.FindStringSubmatch(ua)
		info.Name, info.Family, info.Vendor = "Internet Explorer", "Internet Explorer", "Microsoft Corporation"
		if m[1] != "" {
			info.Version = m[1]
		} else {
			info.Version = m[2]
		}
	default:
		info.Name, info.Family = "Unknown", "Unknown"
	}

	isMobileToken := strings.Contains(ua, "Mobile")
	isTabletHint := strings.Contains(ua, "iPad") || (strings.Contains(ua, "Android") && !isMobileToken)

	switch {
	case reIOS.MatchString(ua):
		m := reIOS.FindStringSubmatch(ua)
		info.OSFamily = "iOS"
		info.OSVersion = strings.ReplaceAll(m[1], "_", ".")
		info.OSVendor = "Apple Inc."
	case reAndroid.MatchString(ua):
		m := reAndroid.FindStringSubmatch(ua)
		info.OSFamily = "Android"
		info.OSVersion = m[1]
		info.OSVendor = "Google Inc."
	case reMac.MatchString(ua):
		m := reMac.FindStringSubmatch(ua)
		info.OSFamily = "Mac OS X"
		info.OSVersion = strings.ReplaceAll(m[1], "_", ".")
		info.OSVendor = "Apple Inc."
	case reWindows.MatchString(ua):
		m := reWindows.FindStringSubmatch(ua)
		info.OSFamily = "Windows"
		if name, ok := windowsVersionNames[m[1]]; ok {
			info.OSVersion = name
		} else {
			info.OSVersion = m[1]
		}
		info.OSVendor = "Microsoft Corporation"
	case reLinux.MatchString(ua):
		info.OSFamily = "Linux"
	default:
		info.OSFamily = "Unknown"
	}

	switch {
	case isTabletHint:
		info.DeviceCategory = "Tablet"
	case isMobileToken:
		info.DeviceCategory = "Smartphone"
	default:
		info.DeviceCategory = "Personal computer"
	}

	if info.DeviceCategory == "Personal computer" {
		info.Type = "Browser"
	} else {
		info.Type = "Mobile Browser"
	}

	return info
}
