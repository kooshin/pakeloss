package controller

import (
	"context"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"pakeloss/internal/model"
	"pakeloss/internal/pb"
)

func TestConnectRejectsDuplicateAgentID(t *testing.T) {
	srv := NewServer(model.ControllerConfig{}, model.MeshConfig{
		ConfigVersion: 1,
		Nodes: []model.NodeConfig{
			{ID: "node-a", UDPAddr: "127.0.0.1:40001"},
			{ID: "node-b", UDPAddr: "127.0.0.1:40002"},
		},
		Flows: []model.MeshFlowConfig{
			{ID: "node-a<=>node-b", Src: "node-a", Dst: "node-b", State: "running", IntervalMs: 10},
		},
	}, nil)

	ctx1, cancel1 := context.WithCancel(context.Background())
	first := newConnectTestStream(ctx1, &pb.Register{
		AgentId: "node-a",
		Token:   "dev-token",
		UdpAddr: "127.0.0.1:40001",
	})
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Connect(first)
	}()

	select {
	case <-first.sentCh:
	case err := <-errCh:
		t.Fatalf("first connect exited early: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first connection to register")
	}

	second := newConnectTestStream(context.Background(), &pb.Register{
		AgentId: "node-a",
		Token:   "dev-token",
		UdpAddr: "127.0.0.1:49999",
	})
	err := srv.Connect(second)
	if status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate connect error code = %v, err = %v, want %v", status.Code(err), err, codes.AlreadyExists)
	}

	cancel1()
	select {
	case err := <-errCh:
		if err != context.Canceled {
			t.Fatalf("first connect shutdown error = %v, want %v", err, context.Canceled)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for first connection shutdown")
	}
}

func TestConnectRejectsInvalidToken(t *testing.T) {
	srv := NewServer(model.ControllerConfig{Token: "expected-token"}, model.MeshConfig{}, nil)
	stream := newConnectTestStream(context.Background(), &pb.Register{
		AgentId: "node-a",
		Token:   "wrong-token",
		UdpAddr: "127.0.0.1:40001",
	})

	err := srv.Connect(stream)
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("connect error code = %v, err = %v, want %v", status.Code(err), err, codes.Unauthenticated)
	}
}

type connectTestStream struct {
	ctx      context.Context
	register *pb.Register
	sentCh   chan struct{}

	mu       sync.Mutex
	recvOnce bool
}

func newConnectTestStream(ctx context.Context, register *pb.Register) *connectTestStream {
	return &connectTestStream{
		ctx:      ctx,
		register: register,
		sentCh:   make(chan struct{}, 1),
	}
}

func (s *connectTestStream) Context() context.Context {
	return s.ctx
}

func (s *connectTestStream) Send(msg *pb.ControllerMessage) error {
	select {
	case s.sentCh <- struct{}{}:
	default:
	}
	return nil
}

func (s *connectTestStream) Recv() (*pb.AgentMessage, error) {
	s.mu.Lock()
	if !s.recvOnce {
		s.recvOnce = true
		s.mu.Unlock()
		return &pb.AgentMessage{Register: s.register}, nil
	}
	s.mu.Unlock()
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func (*connectTestStream) SetHeader(metadata.MD) error  { return nil }
func (*connectTestStream) SendHeader(metadata.MD) error { return nil }
func (*connectTestStream) SetTrailer(metadata.MD)       {}
func (*connectTestStream) SendMsg(any) error            { return nil }
func (*connectTestStream) RecvMsg(any) error            { return nil }
