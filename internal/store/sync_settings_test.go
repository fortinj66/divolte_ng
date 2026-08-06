package store

import (
	"reflect"
	"testing"
)

func TestGetSyncSettingsBeforeSeedingReturnsZeroValueNotError(t *testing.T) {
	s := openTestStore(t)
	got, err := s.GetSyncSettings()
	if err != nil {
		t.Fatalf("GetSyncSettings: %v", err)
	}
	if got.NiFiEnabled || got.DruidEnabled {
		t.Errorf("GetSyncSettings before any seed/set should report both disabled, got %+v", got)
	}
}

func TestEnsureSyncSettingsSeededIsNoOpAfterFirstCall(t *testing.T) {
	s := openTestStore(t)
	if err := s.EnsureSyncSettingsSeeded(); err != nil {
		t.Fatalf("EnsureSyncSettingsSeeded (first): %v", err)
	}
	want := SyncSettings{NiFiEnabled: true, NiFiBaseURL: "https://a.example.com"}
	if err := s.SetSyncSettings(want); err != nil {
		t.Fatalf("SetSyncSettings: %v", err)
	}

	// A second seed call must not overwrite what's already there.
	if err := s.EnsureSyncSettingsSeeded(); err != nil {
		t.Fatalf("EnsureSyncSettingsSeeded (second): %v", err)
	}
	got, err := s.GetSyncSettings()
	if err != nil {
		t.Fatalf("GetSyncSettings: %v", err)
	}
	if got.NiFiBaseURL != want.NiFiBaseURL {
		t.Errorf("GetSyncSettings.NiFiBaseURL = %q, want %q (re-seeding should be a no-op)", got.NiFiBaseURL, want.NiFiBaseURL)
	}
}

func TestSetSyncSettingsRoundTripsAndReplacesWholesale(t *testing.T) {
	s := openTestStore(t)
	want := SyncSettings{
		NiFiEnabled:             true,
		NiFiBaseURL:             "https://nifi-01.example.com:9443",
		NiFiClientCertPEM:       "-----BEGIN CERTIFICATE-----\nfake\n-----END CERTIFICATE-----",
		NiFiClientKeyPEM:        "-----BEGIN PRIVATE KEY-----\nfake\n-----END PRIVATE KEY-----",
		NiFiCACertPEM:           "",
		NiFiParameterContextID:  "f674e954-0180-1000-0000-000001373986",
		NiFiParameterName:       "NiFiAvroSchema",
		NiFiControllerServiceID: "9d173ac1-31dd-14fc-ffff-fffffd818e5d",
		DruidEnabled:            true,
		DruidBaseURL:            "http://druid-01.example.com:8888",
		DruidSupervisorName:     "example-web-metrics",
	}
	if err := s.SetSyncSettings(want); err != nil {
		t.Fatalf("SetSyncSettings: %v", err)
	}
	got, err := s.GetSyncSettings()
	if err != nil {
		t.Fatalf("GetSyncSettings: %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("GetSyncSettings = %+v, want %+v", got, want)
	}

	replacement := SyncSettings{}
	if err := s.SetSyncSettings(replacement); err != nil {
		t.Fatalf("SetSyncSettings (replacement): %v", err)
	}
	got, err = s.GetSyncSettings()
	if err != nil {
		t.Fatalf("GetSyncSettings after replacement: %v", err)
	}
	if !reflect.DeepEqual(got, replacement) {
		t.Errorf("GetSyncSettings after replacement = %+v, want %+v (SetSyncSettings must fully replace, not merge)", got, replacement)
	}
}
