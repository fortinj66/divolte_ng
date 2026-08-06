package uaparse

import "testing"

func TestParseCommonUserAgents(t *testing.T) {
	cases := []struct {
		name          string
		ua            string
		wantFamily    string
		wantOSFamily  string
		wantDeviceCat string
	}{
		{
			name:          "Windows Chrome desktop",
			ua:            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36",
			wantFamily:    "Chrome",
			wantOSFamily:  "Windows",
			wantDeviceCat: "Personal computer",
		},
		{
			name:          "Mac Firefox desktop",
			ua:            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7; rv:109.0) Gecko/20100101 Firefox/115.0",
			wantFamily:    "Firefox",
			wantOSFamily:  "Mac OS X",
			wantDeviceCat: "Personal computer",
		},
		{
			name:          "iPhone Safari mobile",
			ua:            "Mozilla/5.0 (iPhone; CPU iPhone OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Mobile/15E148 Safari/604.1",
			wantFamily:    "Safari",
			wantOSFamily:  "iOS",
			wantDeviceCat: "Smartphone",
		},
		{
			name:          "Android Chrome mobile",
			ua:            "Mozilla/5.0 (Linux; Android 13; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Mobile Safari/537.36",
			wantFamily:    "Chrome",
			wantOSFamily:  "Android",
			wantDeviceCat: "Smartphone",
		},
		{
			name:          "iPad Safari tablet",
			ua:            "Mozilla/5.0 (iPad; CPU OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/604.1",
			wantFamily:    "Safari",
			wantOSFamily:  "iOS",
			wantDeviceCat: "Tablet",
		},
		{
			name:          "Windows Edge desktop",
			ua:            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36 Edg/115.0.1901.183",
			wantFamily:    "Edge",
			wantOSFamily:  "Windows",
			wantDeviceCat: "Personal computer",
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			info := Parse(c.ua)
			if info.Family != c.wantFamily {
				t.Errorf("Family = %q, want %q", info.Family, c.wantFamily)
			}
			if info.OSFamily != c.wantOSFamily {
				t.Errorf("OSFamily = %q, want %q", info.OSFamily, c.wantOSFamily)
			}
			if info.DeviceCategory != c.wantDeviceCat {
				t.Errorf("DeviceCategory = %q, want %q", info.DeviceCategory, c.wantDeviceCat)
			}
			if info.Version == "" {
				t.Errorf("Version should not be empty for %q", c.ua)
			}
		})
	}
}

func TestParseUnknownUserAgentDoesNotPanic(t *testing.T) {
	info := Parse("")
	if info.Name != "Unknown" {
		t.Errorf("Name = %q, want Unknown", info.Name)
	}
	info = Parse("some-bot/1.0 (+http://example.com/bot)")
	if info.Name != "Unknown" {
		t.Errorf("Name = %q, want Unknown for unrecognized bot UA", info.Name)
	}
}
