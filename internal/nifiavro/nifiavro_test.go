package nifiavro

import (
	"testing"

	"github.com/example/divolte-rewrite/internal/nifi"
)

func TestNewRequiresParameterContextID(t *testing.T) {
	_, err := New(Config{
		NiFi:          nifi.Config{BaseURL: "https://example.invalid", ClientCertPEM: "x", ClientKeyPEM: "y"},
		ParameterName: "NiFiAvroSchema",
	})
	if err == nil {
		t.Error("New with no ParameterContextID should error")
	}
}

func TestNewRequiresParameterName(t *testing.T) {
	_, err := New(Config{
		NiFi:               nifi.Config{BaseURL: "https://example.invalid", ClientCertPEM: "x", ClientKeyPEM: "y"},
		ParameterContextID: "abc-123",
	})
	if err == nil {
		t.Error("New with no ParameterName should error")
	}
}

func TestNewPropagatesNiFiClientValidation(t *testing.T) {
	_, err := New(Config{
		NiFi:               nifi.Config{}, // missing BaseURL/cert/key
		ParameterContextID: "abc-123",
		ParameterName:      "NiFiAvroSchema",
	})
	if err == nil {
		t.Error("New should propagate the underlying nifi.NewClient validation error")
	}
}

func TestName(t *testing.T) {
	p := &Plugin{}
	if got := p.Name(); got != "NiFi Avro" {
		t.Errorf("Name() = %q, want %q", got, "NiFi Avro")
	}
}
