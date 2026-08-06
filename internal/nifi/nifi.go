// Package nifi provides generic NiFi REST API primitives - connecting via
// mTLS, walking a controller service's dependency graph, stopping/
// disabling components bottom-up and restoring them top-down, and
// updating a Parameter Context's parameter value. None of this is
// specific to Avro schemas or any particular flow - see internal/nifiavro
// for the plugin that uses these primitives for the Divolte Avro schema
// specifically. A future integration targeting a different NiFi
// parameter or component reuses this same package.
//
// The dependency-chain handling exists because NiFi's REST API does NOT
// orchestrate a parameter update around active dependents on its own -
// confirmed against a real cluster, an update-request is flatly REJECTED
// while any referencing component is active ("Cannot update parameter ...
// because it is referenced by ... which currently has a state of
// ENABLED"). NiFi's own UI appears to "just handle this" because the UI
// CLIENT performs the multi-step dance (list affected components, stop/
// disable them, apply the change, restart/re-enable) before calling the
// same update-requests API this package uses.
package nifi

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/youmark/pkcs8"
)

// Config configures the connection to one NiFi cluster.
type Config struct {
	// BaseURL is the NiFi cluster's own API base, e.g.
	// "https://nifi-01.example.com:9443".
	BaseURL string

	// ClientCertPEM/ClientKeyPEM are an mTLS client certificate NiFi's
	// authorizer has granted API access to (see this environment's
	// existing nifi-monitor-bot automation identity).
	ClientCertPEM string
	ClientKeyPEM  string

	// ClientKeyPassphrase decrypts ClientKeyPEM if it's a
	// passphrase-protected key - either the modern PKCS#8 "ENCRYPTED
	// PRIVATE KEY" format (what you get extracting a key from NiFi
	// toolkit's own .p12 keystore via
	// "openssl pkcs12 -nocerts -in nifi-monitor-bot.p12") or the
	// traditional OpenSSL "Proc-Type: 4,ENCRYPTED" PEM format. Leave
	// empty for an already-unencrypted key; required if the key is
	// encrypted, or NewClient errors with a clear message instead of a
	// cryptic TLS parse failure.
	ClientKeyPassphrase string

	// CACertPEM verifies NiFi's own server certificate. Empty disables
	// verification (InsecureSkipVerify) - matching how this environment's
	// existing automation already connects to this same cluster (curl
	// -k), since the internal CA isn't in this container's trust store by
	// default.
	CACertPEM string

	Timeout time.Duration // defaults to 30s
}

// Client performs the actual NiFi API calls.
type Client struct {
	cfg    Config
	client *http.Client
}

// NewClient validates cfg and builds a Client - does not connect to NiFi
// yet.
func NewClient(cfg Config) (*Client, error) {
	if cfg.BaseURL == "" {
		return nil, fmt.Errorf("nifi: base URL is required")
	}
	if cfg.ClientCertPEM == "" || cfg.ClientKeyPEM == "" {
		return nil, fmt.Errorf("nifi: client cert and key are required")
	}

	keyPEM, err := decryptKeyPEMIfNeeded([]byte(cfg.ClientKeyPEM), cfg.ClientKeyPassphrase)
	if err != nil {
		return nil, fmt.Errorf("nifi: %w", err)
	}
	cert, err := tls.X509KeyPair([]byte(cfg.ClientCertPEM), keyPEM)
	if err != nil {
		return nil, fmt.Errorf("nifi: parsing client cert/key: %w", err)
	}
	tlsCfg := &tls.Config{Certificates: []tls.Certificate{cert}}
	if cfg.CACertPEM != "" {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM([]byte(cfg.CACertPEM)) {
			return nil, fmt.Errorf("nifi: parsing CA cert: no valid certificates found")
		}
		tlsCfg.RootCAs = pool
	} else {
		tlsCfg.InsecureSkipVerify = true
	}

	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	client := &http.Client{
		Timeout:   timeout,
		Transport: &http.Transport{TLSClientConfig: tlsCfg},
	}
	return &Client{cfg: cfg, client: client}, nil
}

// decryptKeyPEMIfNeeded decrypts keyPEM if it's a passphrase-protected PEM
// block, using passphrase. Two encrypted formats are recognized: modern
// PKCS#8 ("ENCRYPTED PRIVATE KEY", PBES2 - what a key extracted from a
// .p12 keystore via "openssl pkcs12 -nocerts" looks like) and the
// traditional OpenSSL PEM encryption ("Proc-Type: 4,ENCRYPTED" on an
// "RSA PRIVATE KEY" block). An already-unencrypted key is returned
// unchanged (passphrase is ignored in that case, so supplying one
// harmlessly for a key that doesn't need it is fine). Errors clearly if
// the key IS encrypted but no passphrase was given, rather than letting
// tls.X509KeyPair fail later with a much more confusing parse error.
func decryptKeyPEMIfNeeded(keyPEM []byte, passphrase string) ([]byte, error) {
	block, _ := pem.Decode(keyPEM)
	if block == nil {
		return keyPEM, nil // not valid PEM at all - let tls.X509KeyPair report that
	}

	if block.Type == "ENCRYPTED PRIVATE KEY" {
		if passphrase == "" {
			return nil, fmt.Errorf("private key is passphrase-protected but no passphrase was supplied")
		}
		key, err := pkcs8.ParsePKCS8PrivateKey(block.Bytes, []byte(passphrase))
		if err != nil {
			return nil, fmt.Errorf("decrypting PKCS#8 private key: %w", err)
		}
		der, err := x509.MarshalPKCS8PrivateKey(key)
		if err != nil {
			return nil, fmt.Errorf("re-encoding decrypted private key: %w", err)
		}
		return pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der}), nil
	}

	//nolint:staticcheck // x509.IsEncryptedPEMBlock/DecryptPEMBlock are deprecated
	// (the classic PEM encryption scheme is weak) but still the correct,
	// working way to decrypt this exact format - there is no non-deprecated
	// stdlib replacement for it as of Go 1.19.
	if !x509.IsEncryptedPEMBlock(block) {
		return keyPEM, nil
	}
	if passphrase == "" {
		return nil, fmt.Errorf("private key is passphrase-protected but no passphrase was supplied")
	}
	der, err := x509.DecryptPEMBlock(block, []byte(passphrase))
	if err != nil {
		return nil, fmt.Errorf("decrypting private key: %w", err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: block.Type, Bytes: der}), nil
}

// ChangedComponent records one component StopDependencyChain
// stopped/disabled, in the order changed, so RestoreChain can reverse it
// correctly. Reversing the list (last-changed-first) restores dependency
// order correctly: a controller service is re-enabled before the
// processor(s) that depend on it are restarted, since NiFi won't start a
// processor whose controller service dependency isn't enabled yet.
type ChangedComponent struct {
	ID   string
	Kind string // "Processor" or "ControllerService"
}

// StopDependencyChain recursively walks the referencingComponents graph
// rooted at serviceID, stopping every RUNNING processor found (directly,
// or via an intermediate controller service) and disabling every ENABLED
// controller service found along the way - depth-first, a controller
// service's OWN referencing processors are stopped before that service
// itself is disabled, since NiFi won't disable a service anything is
// still actively using. Only components that were ACTUALLY running/
// enabled are touched and returned - anything already stopped/disabled
// is left alone entirely, so RestoreChain never restarts something that
// wasn't running before this ran. serviceID itself is NOT included in
// the returned list (the caller disables/re-enables that one directly,
// since it's the root the caller already knows about) - only its
// dependents.
func (c *Client) StopDependencyChain(serviceID string) ([]ChangedComponent, error) {
	var changed []ChangedComponent
	if err := c.stopDependencyChainInto(serviceID, &changed, map[string]bool{serviceID: true}); err != nil {
		return changed, err
	}
	return changed, nil
}

func (c *Client) stopDependencyChainInto(serviceID string, changed *[]ChangedComponent, visited map[string]bool) error {
	var entity struct {
		Component struct {
			ReferencingComponents []referencingComponentEntity `json:"referencingComponents"`
		} `json:"component"`
	}
	if err := c.doJSON(http.MethodGet, "/nifi-api/controller-services/"+serviceID, nil, &entity); err != nil {
		return fmt.Errorf("getting referencing components of %s: %w", serviceID, err)
	}

	for _, rc := range entity.Component.ReferencingComponents {
		id := rc.Component.ID
		if visited[id] {
			continue
		}
		visited[id] = true

		switch rc.Component.ReferenceType {
		case "Processor":
			if rc.Component.State == "RUNNING" {
				if err := c.SetProcessorState(id, "STOPPED"); err != nil {
					return fmt.Errorf("stopping processor %s: %w", id, err)
				}
				*changed = append(*changed, ChangedComponent{ID: id, Kind: "Processor"})
			}
		case "ControllerService":
			// This service's OWN dependents (processors using IT) must be
			// stopped before IT can be disabled - recurse first.
			if err := c.stopDependencyChainInto(id, changed, visited); err != nil {
				return err
			}
			if rc.Component.State == "ENABLED" {
				if err := c.SetControllerServiceState(id, "DISABLED"); err != nil {
					return fmt.Errorf("disabling controller service %s: %w", id, err)
				}
				*changed = append(*changed, ChangedComponent{ID: id, Kind: "ControllerService"})
			}
		}
	}
	return nil
}

// RestoreChain reverses changed in REVERSE order (last-changed first) -
// see ChangedComponent's doc comment for why that order is correct.
// Collects every error rather than stopping at the first, so one stuck
// component doesn't prevent restoring the rest.
func (c *Client) RestoreChain(changed []ChangedComponent) error {
	var errs []string
	for i := len(changed) - 1; i >= 0; i-- {
		comp := changed[i]
		var err error
		switch comp.Kind {
		case "Processor":
			err = c.SetProcessorState(comp.ID, "RUNNING")
		case "ControllerService":
			err = c.SetControllerServiceState(comp.ID, "ENABLED")
		}
		if err != nil {
			errs = append(errs, fmt.Sprintf("restoring %s %s: %v", comp.Kind, comp.ID, err))
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

// SetControllerServiceState is a no-op if the service is already in the
// desired state ("ENABLED" or "DISABLED"). NiFi requires the service's
// current revision for any state-changing PUT, and transitions go
// through transient ENABLING/DISABLING states, so this polls until the
// desired state is actually reached (or times out).
func (c *Client) SetControllerServiceState(id, desiredState string) error {
	var entity revisionedStateEntity
	if err := c.doJSON(http.MethodGet, "/nifi-api/controller-services/"+id, nil, &entity); err != nil {
		return fmt.Errorf("getting controller service: %w", err)
	}
	if entity.Component.State == desiredState {
		return nil
	}
	if err := c.putState("/nifi-api/controller-services/"+id, id, entity.Revision, desiredState); err != nil {
		return fmt.Errorf("setting controller service state to %s: %w", desiredState, err)
	}
	return c.pollState("/nifi-api/controller-services/"+id, desiredState)
}

// SetProcessorState mirrors SetControllerServiceState for a processor
// ("RUNNING" or "STOPPED" instead of "ENABLED"/"DISABLED").
func (c *Client) SetProcessorState(id, desiredState string) error {
	var entity revisionedStateEntity
	if err := c.doJSON(http.MethodGet, "/nifi-api/processors/"+id, nil, &entity); err != nil {
		return fmt.Errorf("getting processor: %w", err)
	}
	if entity.Component.State == desiredState {
		return nil
	}
	if err := c.putState("/nifi-api/processors/"+id, id, entity.Revision, desiredState); err != nil {
		return fmt.Errorf("setting processor state to %s: %w", desiredState, err)
	}
	return c.pollState("/nifi-api/processors/"+id, desiredState)
}

func (c *Client) putState(path, id string, rev revision, desiredState string) error {
	body := map[string]interface{}{
		"revision": rev,
		"component": map[string]interface{}{
			"id":    id,
			"state": desiredState,
		},
	}
	return c.doJSON(http.MethodPut, path, body, nil)
}

func (c *Client) pollState(path, desiredState string) error {
	deadline := time.Now().Add(c.timeout())
	for {
		var check revisionedStateEntity
		if err := c.doJSON(http.MethodGet, path, nil, &check); err != nil {
			return fmt.Errorf("polling state: %w", err)
		}
		if check.Component.State == desiredState {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("did not reach state %s within %s (currently %s)", desiredState, c.timeout(), check.Component.State)
		}
		time.Sleep(1 * time.Second)
	}
}

// GetControllerServiceInfo reports a controller service's current state
// and how many components directly reference it - used by a Test-
// connection button to show something meaningful without changing
// anything.
func (c *Client) GetControllerServiceInfo(id string) (state string, dependentCount int, err error) {
	var entity struct {
		Component struct {
			State                 string                       `json:"state"`
			ReferencingComponents []referencingComponentEntity `json:"referencingComponents"`
		} `json:"component"`
	}
	if err := c.doJSON(http.MethodGet, "/nifi-api/controller-services/"+id, nil, &entity); err != nil {
		return "", 0, err
	}
	return entity.Component.State, len(entity.Component.ReferencingComponents), nil
}

// ParameterContextRevision returns a parameter context's current
// revision version - required by UpdateParameterContext for optimistic
// locking, and useful on its own for a Test-connection button to prove
// connectivity.
func (c *Client) ParameterContextRevision(parameterContextID string) (int64, error) {
	var entity paramContextEntity
	if err := c.doJSON(http.MethodGet, "/nifi-api/parameter-contexts/"+parameterContextID, nil, &entity); err != nil {
		return 0, fmt.Errorf("getting parameter context revision: %w", err)
	}
	return entity.Revision.Version, nil
}

// UpdateParameterContext sets parameterName's value within
// parameterContextID to value, submitting NiFi's asynchronous update-
// request and polling it to completion. This is generic - it doesn't
// know or care that the value happens to be an Avro schema; see
// internal/nifiavro for the piece that's specific to that.
func (c *Client) UpdateParameterContext(parameterContextID, parameterName, value string) error {
	ver, err := c.ParameterContextRevision(parameterContextID)
	if err != nil {
		return err
	}

	reqBody := updateRequestBody{
		ID:       parameterContextID,
		Revision: revision{Version: ver},
		Component: updateComponent{
			ID: parameterContextID,
			Parameters: []parameterWrap{{Parameter: parameterValue{
				Name: parameterName, Value: value, Sensitive: false,
			}}},
		},
	}
	var entity updateRequestEntity
	if err := c.doJSON(http.MethodPost, "/nifi-api/parameter-contexts/"+parameterContextID+"/update-requests", reqBody, &entity); err != nil {
		return fmt.Errorf("submitting parameter update: %w", err)
	}
	requestID := entity.Request.RequestID

	deadline := time.Now().Add(c.timeout())
	for !entity.Request.Complete {
		if time.Now().After(deadline) {
			return fmt.Errorf("parameter update did not complete within %s", c.timeout())
		}
		time.Sleep(1 * time.Second)
		if err := c.doJSON(http.MethodGet, "/nifi-api/parameter-contexts/"+parameterContextID+"/update-requests/"+requestID, nil, &entity); err != nil {
			return fmt.Errorf("polling update request: %w", err)
		}
	}

	// Best-effort acknowledge/clean-up of the completed request - NiFi's
	// own UI does this too, but failing to isn't a reason to report the
	// value update itself as failed.
	_ = c.doJSON(http.MethodDelete, "/nifi-api/parameter-contexts/"+parameterContextID+"/update-requests/"+requestID, nil, nil)

	if entity.Request.FailureReason != "" {
		return fmt.Errorf("parameter update failed: %s", entity.Request.FailureReason)
	}
	return nil
}

func (c *Client) timeout() time.Duration {
	if c.cfg.Timeout > 0 {
		return c.cfg.Timeout
	}
	return 30 * time.Second
}

func (c *Client) doJSON(method, path string, body, out interface{}) error {
	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("encoding request body: %w", err)
		}
		reqBody = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, c.cfg.BaseURL+path, reqBody)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("%s %s: HTTP %d: %s", method, path, resp.StatusCode, string(respBody))
	}
	if out != nil && len(respBody) > 0 {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("decoding response from %s %s: %w", method, path, err)
		}
	}
	return nil
}

type revision struct {
	Version int64 `json:"version"`
}

type paramContextEntity struct {
	Revision revision `json:"revision"`
}

// revisionedStateEntity covers both processors and controller services -
// both entity shapes carry revision + component.state, just with
// different allowed state values (RUNNING/STOPPED vs ENABLED/DISABLED).
type revisionedStateEntity struct {
	Revision  revision `json:"revision"`
	Component struct {
		State string `json:"state"`
	} `json:"component"`
}

// updateRequestBody mirrors NiFi's ParameterContextEntity shape - the
// parameter context's ID is required at the TOP level (matching the URL
// path parameter) as well as inside "component"; a cluster node rejected
// a request that only had it nested ("The ID of the Parameter Context
// must be specified"), confirmed against the real cluster.
type updateRequestBody struct {
	ID        string          `json:"id"`
	Revision  revision        `json:"revision"`
	Component updateComponent `json:"component"`
}

type updateComponent struct {
	ID         string          `json:"id"`
	Parameters []parameterWrap `json:"parameters"`
}

type parameterWrap struct {
	Parameter parameterValue `json:"parameter"`
}

type parameterValue struct {
	Name      string `json:"name"`
	Value     string `json:"value"`
	Sensitive bool   `json:"sensitive"`
}

type updateRequestEntity struct {
	Request struct {
		RequestID     string `json:"requestId"`
		Complete      bool   `json:"complete"`
		FailureReason string `json:"failureReason"`
	} `json:"request"`
}

// referencingComponentEntity is one entry in a controller service's
// referencingComponents list - confirmed against the real cluster to
// carry "referenceType" as either "ControllerService" or "Processor"
// (exact casing), and "state" as ENABLED/DISABLED or RUNNING/STOPPED
// respectively depending on which.
type referencingComponentEntity struct {
	Revision  revision `json:"revision"`
	Component struct {
		ID            string `json:"id"`
		ReferenceType string `json:"referenceType"`
		State         string `json:"state"`
	} `json:"component"`
}
