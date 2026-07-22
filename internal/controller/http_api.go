package controller

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strings"
	"time"

	"pakeloss/internal/model"
)

func (s *Server) ListenAndServeHTTP(ctx context.Context) error {
	server := &http.Server{
		Addr:              s.cfg.HTTPAddr,
		Handler:           s.httpHandler(),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	go func() {
		<-ctx.Done()
		_ = server.Shutdown(context.Background())
	}()
	log.Printf("controller http listening: %s", s.cfg.HTTPAddr)
	return server.ListenAndServe()
}

func (s *Server) httpHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api/v1/status", s.handleStatus)
	mux.HandleFunc("/api/v1/agents", s.handleAgents)
	mux.HandleFunc("/api/v1/agents/", s.handleAgentPath)
	mux.HandleFunc("/api/v1/flows", s.handleFlows)
	mux.HandleFunc("/api/v1/flows/", s.handleFlowPath)
	return s.requireHTTPAuth(mux)
}

func (s *Server) requireHTTPAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !s.isAuthorizedHTTPRequest(r) {
			writeJSON(w, http.StatusUnauthorized, map[string]any{"status": "error", "message": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) isAuthorizedHTTPRequest(r *http.Request) bool {
	if s.cfg.Token == "" {
		return true
	}
	auth := strings.TrimSpace(r.Header.Get("Authorization"))
	if !strings.HasPrefix(auth, "Bearer ") {
		return false
	}
	return secureTokenEqual(strings.TrimSpace(strings.TrimPrefix(auth, "Bearer ")), s.cfg.Token)
}

func (s *Server) handleAgentPath(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/agents/")
	parts := strings.Split(path, "/")
	if len(parts) == 2 {
		agentID, action := parts[0], parts[1]
		var enabled bool
		switch action {
		case "enable":
			enabled = true
		case "disable":
			enabled = false
		default:
			http.NotFound(w, r)
			return
		}
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		if err := s.SetAgentEnabled(agentID, enabled); err != nil {
			switch err {
			case ErrAgentNotFound:
				writeJSON(w, http.StatusNotFound, map[string]any{"status": "error", "message": err.Error()})
			case ErrAutoDiscoveryOnly, ErrAllFlowsMustBeStopped:
				writeJSON(w, http.StatusConflict, map[string]any{"status": "error", "message": err.Error()})
			default:
				writeJSON(w, http.StatusInternalServerError, map[string]any{"status": "error", "message": err.Error()})
			}
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
		return
	}
	http.NotFound(w, r)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	writeJSON(w, http.StatusOK, model.StatusSnapshot{MeasurementSessionID: s.CurrentSessionID()})
}

func (s *Server) handleAgents(w http.ResponseWriter, r *http.Request) {
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	agents := s.runtime.Agents()
	out := make([]model.AgentSnapshot, 0, len(agents))
	for _, agent := range agents {
		out = append(out, model.NewAgentSnapshot(agent))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleFlows(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/api/v1/flows" {
		http.NotFound(w, r)
		return
	}
	if !requireMethod(w, r, http.MethodGet) {
		return
	}
	flows := s.runtime.Flows()
	out := make([]model.FlowSnapshot, 0, len(flows))
	for _, flow := range flows {
		out = append(out, model.NewFlowSnapshot(flow))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleFlowPath(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/v1/flows/")
	if path == "start" {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		s.SetAllFlowStates("running")
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
		return
	}
	if path == "stop" {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		s.SetAllFlowStates("stopped")
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
		return
	}
	if path == "restart" {
		if !requireMethod(w, r, http.MethodPost) {
			return
		}
		s.RestartAllFlows()
		writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
		return
	}
	parts := strings.Split(path, "/")
	if len(parts) == 2 {
		flowID, action := parts[0], parts[1]
		switch action {
		case "resume":
			if !requireMethod(w, r, http.MethodPost) {
				return
			}
			if err := s.SetFlowState(flowID, "running"); err != nil {
				writeJSON(w, http.StatusNotFound, map[string]any{"status": "error", "message": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
			return
		case "pause":
			if !requireMethod(w, r, http.MethodPost) {
				return
			}
			if err := s.SetFlowState(flowID, "stopped"); err != nil {
				writeJSON(w, http.StatusNotFound, map[string]any{"status": "error", "message": err.Error()})
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"status": "ok"})
			return
		}
	}
	http.NotFound(w, r)
}

func requireMethod(w http.ResponseWriter, r *http.Request, method string) bool {
	if r.Method == method {
		return true
	}
	w.Header().Set("Allow", method)
	writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"status": "error", "message": "method not allowed"})
	return false
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
