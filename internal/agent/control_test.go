package agent

import (
	"errors"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsPermanentConnectError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{
			name: "duplicate agent id sentinel",
			err:  errDuplicateAgentID,
			want: true,
		},
		{
			name: "grpc already exists",
			err:  status.Error(codes.AlreadyExists, "agent_id already connected: node-a"),
			want: true,
		},
		{
			name: "joined duplicate error",
			err:  errors.Join(errDuplicateAgentID, status.Error(codes.AlreadyExists, "agent_id already connected: node-a")),
			want: true,
		},
		{
			name: "transient unavailable",
			err:  status.Error(codes.Unavailable, "controller unavailable"),
			want: false,
		},
		{
			name: "nil",
			err:  nil,
			want: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := isPermanentConnectError(tc.err); got != tc.want {
				t.Fatalf("isPermanentConnectError(%v) = %v, want %v", tc.err, got, tc.want)
			}
		})
	}
}
