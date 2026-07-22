package tui

import (
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestClientAddsAuthorizationHeader(t *testing.T) {
	client := &Client{
		base:  "http://controller.test",
		token: "dev-token",
		http: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
			if got := r.Header.Get("Authorization"); got != "Bearer dev-token" {
				t.Fatalf("authorization = %q, want Bearer dev-token", got)
			}
			return &http.Response{
				StatusCode: http.StatusOK,
				Body:       io.NopCloser(strings.NewReader("[]")),
			}, nil
		})},
	}

	if _, err := client.Flows(); err != nil {
		t.Fatal(err)
	}
}
