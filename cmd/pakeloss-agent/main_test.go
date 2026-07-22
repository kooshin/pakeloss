package main

import (
	"testing"

	"pakeloss/internal/model"
)

func TestValidateAgentConfigRequiresToken(t *testing.T) {
	base := model.AgentConfig{AgentID: "node-a", ControllerAddr: "127.0.0.1:8443", ListenAddr: "127.0.0.1:40001"}
	if err := validateAgentConfig(base); err == nil {
		t.Fatal("expected missing token error")
	}
	base.Token = "test-token"
	if err := validateAgentConfig(base); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}
