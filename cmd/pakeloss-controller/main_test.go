package main

import (
	"testing"

	"pakeloss/internal/model"
)

func TestValidateControllerConfigRequiresToken(t *testing.T) {
	if err := validateControllerConfig(model.ControllerConfig{}); err == nil {
		t.Fatal("expected missing token error")
	}
	if err := validateControllerConfig(model.ControllerConfig{Token: "test-token"}); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
}
