package store

import (
	"reflect"
	"testing"
)

func TestNiFiAvroTargetCRUD(t *testing.T) {
	s := openTestStore(t)

	if got, err := s.ListNiFiAvroTargets(); err != nil || len(got) != 0 {
		t.Fatalf("ListNiFiAvroTargets before any target = %v, %v, want empty, nil", got, err)
	}
	if got, err := s.GetNiFiAvroTarget("nope"); err != nil || got != nil {
		t.Fatalf("GetNiFiAvroTarget(missing) = %v, %v, want nil, nil", got, err)
	}

	want := NiFiAvroTarget{
		ID: "nifi-legacy", Enabled: true, BaseURL: "https://nifi-01.example.com:9443",
		ClientCertPEM: "cert-pem", ClientKeyPEM: "key-pem", CACertPEM: "",
		ParameterContextID: "f674e954-0180-1000-0000-000001373986", ParameterName: "NiFiAvroSchema",
		ControllerServiceID: "9d173ac1-31dd-14fc-ffff-fffffd818e5d",
	}
	if err := s.UpsertNiFiAvroTarget(want); err != nil {
		t.Fatalf("UpsertNiFiAvroTarget: %v", err)
	}

	got, err := s.GetNiFiAvroTarget("nifi-legacy")
	if err != nil || got == nil {
		t.Fatalf("GetNiFiAvroTarget: %v, %v", got, err)
	}
	if !reflect.DeepEqual(*got, want) {
		t.Errorf("GetNiFiAvroTarget = %+v, want %+v", *got, want)
	}

	// A second target, to confirm List returns both, ordered.
	second := want
	second.ID = "nifi-d0x"
	second.BaseURL = "https://nifi-02.example.com:8443"
	if err := s.UpsertNiFiAvroTarget(second); err != nil {
		t.Fatalf("UpsertNiFiAvroTarget (second): %v", err)
	}
	all, err := s.ListNiFiAvroTargets()
	if err != nil {
		t.Fatalf("ListNiFiAvroTargets: %v", err)
	}
	if len(all) != 2 || all[0].ID != "nifi-d0x" || all[1].ID != "nifi-legacy" {
		t.Errorf("ListNiFiAvroTargets = %+v, want 2 targets ordered by id", all)
	}

	// Upsert replaces wholesale.
	want.Enabled = false
	want.BaseURL = "https://changed.example.com:9443"
	if err := s.UpsertNiFiAvroTarget(want); err != nil {
		t.Fatalf("UpsertNiFiAvroTarget (update): %v", err)
	}
	got, err = s.GetNiFiAvroTarget("nifi-legacy")
	if err != nil || got == nil || got.BaseURL != want.BaseURL || got.Enabled != false {
		t.Errorf("GetNiFiAvroTarget after update = %+v, %v, want updated values", got, err)
	}

	if err := s.DeleteNiFiAvroTarget("nifi-legacy"); err != nil {
		t.Fatalf("DeleteNiFiAvroTarget: %v", err)
	}
	if got, err := s.GetNiFiAvroTarget("nifi-legacy"); err != nil || got != nil {
		t.Errorf("GetNiFiAvroTarget after delete = %v, %v, want nil, nil", got, err)
	}
	// Deleting a non-existent target is a no-op, not an error.
	if err := s.DeleteNiFiAvroTarget("never-existed"); err != nil {
		t.Errorf("DeleteNiFiAvroTarget(missing) should not error, got: %v", err)
	}
}

func TestDruidTargetCRUD(t *testing.T) {
	s := openTestStore(t)

	want := DruidTarget{ID: "druid-dev", Enabled: true, BaseURL: "http://druid-01.example.com:8081", SupervisorName: "example-web-metrics"}
	if err := s.UpsertDruidTarget(want); err != nil {
		t.Fatalf("UpsertDruidTarget: %v", err)
	}
	got, err := s.GetDruidTarget("druid-dev")
	if err != nil || got == nil || !reflect.DeepEqual(*got, want) {
		t.Fatalf("GetDruidTarget = %+v, %v, want %+v", got, err, want)
	}

	second := DruidTarget{ID: "druid-prod1", Enabled: false, BaseURL: "http://druid-02.example.com:8081", SupervisorName: "example-web-metrics-prod"}
	if err := s.UpsertDruidTarget(second); err != nil {
		t.Fatalf("UpsertDruidTarget (second): %v", err)
	}
	all, err := s.ListDruidTargets()
	if err != nil || len(all) != 2 {
		t.Fatalf("ListDruidTargets = %+v, %v, want 2 targets", all, err)
	}

	if err := s.DeleteDruidTarget("druid-dev"); err != nil {
		t.Fatalf("DeleteDruidTarget: %v", err)
	}
	all, err = s.ListDruidTargets()
	if err != nil || len(all) != 1 || all[0].ID != "druid-prod1" {
		t.Errorf("ListDruidTargets after delete = %+v, %v", all, err)
	}
}

// TestEnsureTargetsMigratedFromLegacySyncSettings confirms the one-time
// migration converts a populated legacy single-target sync_settings row
// into the first named target, and is a no-op if any target already
// exists (so it never clobbers real targets an admin already created
// through the new multi-target UI).
func TestEnsureTargetsMigratedFromLegacySyncSettings(t *testing.T) {
	s := openTestStore(t)

	if err := s.SetSyncSettings(SyncSettings{
		NiFiEnabled: true, NiFiBaseURL: "https://nifi-01.example.com:9443",
		NiFiClientCertPEM: "cert", NiFiClientKeyPEM: "key",
		NiFiParameterContextID: "ctx-1", NiFiParameterName: "NiFiAvroSchema", NiFiControllerServiceID: "svc-1",
		DruidEnabled: true, DruidBaseURL: "http://druid-01.example.com:8081", DruidSupervisorName: "example-web-metrics",
	}); err != nil {
		t.Fatalf("SetSyncSettings: %v", err)
	}

	if err := s.EnsureTargetsMigratedFromLegacySyncSettings(); err != nil {
		t.Fatalf("EnsureTargetsMigratedFromLegacySyncSettings: %v", err)
	}

	nifiTargets, err := s.ListNiFiAvroTargets()
	if err != nil || len(nifiTargets) != 1 || nifiTargets[0].ID != "nifi-legacy" || nifiTargets[0].BaseURL != "https://nifi-01.example.com:9443" {
		t.Fatalf("ListNiFiAvroTargets after migration = %+v, %v, want one nifi-legacy target", nifiTargets, err)
	}
	druidTargets, err := s.ListDruidTargets()
	if err != nil || len(druidTargets) != 1 || druidTargets[0].ID != "druid-dev" {
		t.Fatalf("ListDruidTargets after migration = %+v, %v, want one druid-dev target", druidTargets, err)
	}

	// Running it again must NOT duplicate or overwrite - simulates a
	// second instance also calling this at its own startup.
	if err := s.UpsertNiFiAvroTarget(NiFiAvroTarget{ID: "nifi-legacy", BaseURL: "https://manually-edited.example.com"}); err != nil {
		t.Fatalf("UpsertNiFiAvroTarget: %v", err)
	}
	if err := s.EnsureTargetsMigratedFromLegacySyncSettings(); err != nil {
		t.Fatalf("EnsureTargetsMigratedFromLegacySyncSettings (second run): %v", err)
	}
	nifiTargets, err = s.ListNiFiAvroTargets()
	if err != nil || len(nifiTargets) != 1 || nifiTargets[0].BaseURL != "https://manually-edited.example.com" {
		t.Errorf("second migration run overwrote an existing target: %+v, %v", nifiTargets, err)
	}
}

func TestEnsureTargetsMigratedIsNoOpWithNoLegacyConfig(t *testing.T) {
	s := openTestStore(t)
	if err := s.EnsureTargetsMigratedFromLegacySyncSettings(); err != nil {
		t.Fatalf("EnsureTargetsMigratedFromLegacySyncSettings with no legacy config: %v", err)
	}
	nifiTargets, err := s.ListNiFiAvroTargets()
	if err != nil || len(nifiTargets) != 0 {
		t.Errorf("expected no targets created from empty legacy config, got %+v, %v", nifiTargets, err)
	}
}
