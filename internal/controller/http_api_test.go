package controller

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"pakeloss/internal/model"
	"pakeloss/internal/pb"
)

func TestRestartAllReturnsOK(t *testing.T) {
	srv := NewServer(model.ControllerConfig{}, model.MeshConfig{
		Flows: []model.MeshFlowConfig{{ID: "node-a<=>node-b", Src: "node-a", Dst: "node-b"}},
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/flows/restart", nil)
	rec := httptest.NewRecorder()
	srv.handleFlowPath(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if body := rec.Body.String(); body == "" {
		t.Fatal("expected response body")
	}
}

func TestHistoryClearReturnsNotFound(t *testing.T) {
	srv := NewServer(model.ControllerConfig{}, model.MeshConfig{}, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/flows/history/clear", nil)
	rec := httptest.NewRecorder()
	srv.handleFlowPath(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestFlowsResponseIncludesLossHistory240s(t *testing.T) {
	srv := NewServer(model.ControllerConfig{}, model.MeshConfig{
		Flows: []model.MeshFlowConfig{{ID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", State: "running", IntervalMs: 10}},
	}, nil)
	srv.runtime.IngestResult(&pb.ResultSummary{FlowId: "node-a<=>node-b", AgentId: "node-b", Src: "node-a", Dst: "node-b", Tx: 100, Rx: 99, Lost: 1, LossRatio: 0.01})

	req := httptest.NewRequest(http.MethodGet, "/api/v1/flows", nil)
	rec := httptest.NewRecorder()
	srv.handleFlows(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), "\"loss_history_240s\"") {
		t.Fatalf("response missing loss_history_240s: %s", rec.Body.String())
	}
	if strings.Contains(rec.Body.String(), "\"down_total_ms\"") {
		t.Fatalf("response contains removed down_total_ms: %s", rec.Body.String())
	}
	for _, field := range []string{
		"interval_ms",
		"isolated_loss_events",
		"outage_count",
		"outage_active",
		"current_outage_ms",
		"last_outage_ms",
		"max_outage_ms",
		"outage_total_ms",
		"outage_threshold_ms",
		"loss_time_1s_ms",
		"loss_time_total_ms",
		"lost_total",
		"duplicate_total",
		"reorder_total",
		"last_seen",
	} {
		if !strings.Contains(rec.Body.String(), `"`+field+`"`) {
			t.Fatalf("response missing TUI field %q: %s", field, rec.Body.String())
		}
	}
	for _, field := range []string{
		"receiver_agent_id",
		"dscp",
		"source_port_count",
		"loss_history_10s",
		"loss_history_20s",
		"consecutive_loss_seconds",
		"history_cleared_at",
	} {
		if strings.Contains(rec.Body.String(), `"`+field+`"`) {
			t.Fatalf("response includes unused TUI field %q: %s", field, rec.Body.String())
		}
	}
}

func TestStatusResponseIncludesMeasurementSessionID(t *testing.T) {
	srv := NewServer(model.ControllerConfig{}, model.MeshConfig{
		Flows: []model.MeshFlowConfig{{ID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", State: "running", IntervalMs: 10}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/status", nil)
	rec := httptest.NewRecorder()
	srv.handleStatus(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"measurement_session_id":"`) || strings.Contains(rec.Body.String(), `"measurement_session_id":""`) {
		t.Fatalf("response missing measurement session id: %s", rec.Body.String())
	}
}

func TestAgentsResponseIncludesEnabled(t *testing.T) {
	srv := NewServer(model.ControllerConfig{}, model.MeshConfig{
		DiscoveryMode: "auto",
		Nodes:         []model.NodeConfig{{ID: "node-a", UDPAddr: "127.0.0.1:40001", Enabled: true}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/agents", nil)
	rec := httptest.NewRecorder()
	srv.handleAgents(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"enabled":true`) {
		t.Fatalf("response missing enabled flag: %s", rec.Body.String())
	}
}

func TestDisableAgentReturnsOK(t *testing.T) {
	srv := NewServer(model.ControllerConfig{}, model.MeshConfig{
		ConfigVersion: 1,
		DiscoveryMode: "auto",
		AutoFlow:      model.AutoFlowConfig{State: "stopped"},
	}, nil)
	if _, _, err := srv.configs.UpsertDiscoveredNode("node-a", "127.0.0.1:40001"); err != nil {
		t.Fatal(err)
	}
	mesh, _, err := srv.configs.UpsertDiscoveredNode("node-b", "127.0.0.1:40002")
	if err != nil {
		t.Fatal(err)
	}
	srv.runtime.SetDesiredConfigVersion(mesh)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/node-b/disable", nil)
	rec := httptest.NewRecorder()
	srv.handleAgentPath(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	flows := srv.runtime.Flows()
	if len(flows) != 0 {
		t.Fatalf("flows after disable = %+v, want none", flows)
	}
	agents := srv.runtime.Agents()
	found := false
	for _, agent := range agents {
		if agent.AgentID == "node-b" {
			found = true
			if agent.Enabled {
				t.Fatalf("node-b should be disabled: %+v", agent)
			}
		}
	}
	if !found {
		t.Fatal("node-b agent snapshot missing")
	}
}

func TestDisableAgentRejectsRunningFlows(t *testing.T) {
	srv := NewServer(model.ControllerConfig{}, model.MeshConfig{
		ConfigVersion: 1,
		DiscoveryMode: "auto",
	}, nil)
	if _, _, err := srv.configs.UpsertDiscoveredNode("node-a", "127.0.0.1:40001"); err != nil {
		t.Fatal(err)
	}
	mesh, _, err := srv.configs.UpsertDiscoveredNode("node-b", "127.0.0.1:40002")
	if err != nil {
		t.Fatal(err)
	}
	mesh = srv.configs.SetAllFlowStates("running")
	srv.runtime.SetDesiredConfigVersion(mesh)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/node-b/disable", nil)
	rec := httptest.NewRecorder()
	srv.handleAgentPath(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), ErrAllFlowsMustBeStopped.Error()) {
		t.Fatalf("body = %s, want stopped-flow message", rec.Body.String())
	}
}

func TestDisableAgentRejectsStaticMode(t *testing.T) {
	srv := NewServer(model.ControllerConfig{}, model.MeshConfig{
		ConfigVersion: 1,
		Nodes:         []model.NodeConfig{{ID: "node-a", UDPAddr: "127.0.0.1:40001"}},
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/node-a/disable", nil)
	rec := httptest.NewRecorder()
	srv.handleAgentPath(rec, req)

	if rec.Code != http.StatusConflict {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), ErrAutoDiscoveryOnly.Error()) {
		t.Fatalf("body = %s, want auto-only message", rec.Body.String())
	}
}

func TestDisableAgentReturnsNotFound(t *testing.T) {
	srv := NewServer(model.ControllerConfig{}, model.MeshConfig{
		ConfigVersion: 1,
		DiscoveryMode: "auto",
	}, nil)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agents/node-x/disable", nil)
	rec := httptest.NewRecorder()
	srv.handleAgentPath(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), ErrAgentNotFound.Error()) {
		t.Fatalf("body = %s, want agent-not-found message", rec.Body.String())
	}
}

func TestHTTPHandlerRejectsMissingToken(t *testing.T) {
	srv := NewServer(model.ControllerConfig{Token: "dev-token"}, model.MeshConfig{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/flows", nil)
	rec := httptest.NewRecorder()
	srv.httpHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"message":"unauthorized"`) {
		t.Fatalf("body = %s, want unauthorized message", rec.Body.String())
	}
}

func TestHTTPHandlerRejectsWrongToken(t *testing.T) {
	srv := NewServer(model.ControllerConfig{Token: "dev-token"}, model.MeshConfig{}, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/flows", nil)
	req.Header.Set("Authorization", "Bearer wrong-token")
	rec := httptest.NewRecorder()
	srv.httpHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestHTTPHandlerAcceptsBearerToken(t *testing.T) {
	srv := NewServer(model.ControllerConfig{Token: "dev-token"}, model.MeshConfig{
		Flows: []model.MeshFlowConfig{{ID: "node-a<=>node-b", Src: "node-a", Dst: "node-b"}},
	}, nil)

	req := httptest.NewRequest(http.MethodGet, "/api/v1/flows", nil)
	req.Header.Set("Authorization", "Bearer dev-token")
	rec := httptest.NewRecorder()
	srv.httpHandler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
	}
}

func TestHTTPHandlerRejectsWrongMethod(t *testing.T) {
	srv := NewServer(model.ControllerConfig{Token: "dev-token"}, model.MeshConfig{}, nil)

	tests := []struct {
		method string
		path   string
		allow  string
	}{
		{method: http.MethodPost, path: "/api/v1/status", allow: http.MethodGet},
		{method: http.MethodPost, path: "/api/v1/agents", allow: http.MethodGet},
		{method: http.MethodPost, path: "/api/v1/flows", allow: http.MethodGet},
		{method: http.MethodGet, path: "/api/v1/flows/start", allow: http.MethodPost},
		{method: http.MethodGet, path: "/api/v1/agents/node-a/disable", allow: http.MethodPost},
	}
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			req := httptest.NewRequest(tt.method, tt.path, nil)
			req.Header.Set("Authorization", "Bearer dev-token")
			rec := httptest.NewRecorder()
			srv.httpHandler().ServeHTTP(rec, req)
			if rec.Code != http.StatusMethodNotAllowed {
				t.Fatalf("status = %d, body = %s", rec.Code, rec.Body.String())
			}
			if got := rec.Header().Get("Allow"); got != tt.allow {
				t.Fatalf("Allow = %q, want %q", got, tt.allow)
			}
		})
	}
}
