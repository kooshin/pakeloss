package agent

import (
	"context"
	"errors"
	"io"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"pakeloss/internal/model"
	"pakeloss/internal/pb"
)

type ControlClient struct {
	cfg      model.AgentConfig
	manager  *FlowManager
	receiver *UDPReceiver
	results  <-chan *pb.ResultReport
}

func NewControlClient(cfg model.AgentConfig, manager *FlowManager, receiver *UDPReceiver, results <-chan *pb.ResultReport) *ControlClient {
	return &ControlClient{cfg: cfg, manager: manager, receiver: receiver, results: results}
}

var errDuplicateAgentID = errors.New("duplicate agent_id")

func (c *ControlClient) Run(ctx context.Context) error {
	for {
		if err := c.connectOnce(ctx); err != nil && ctx.Err() == nil {
			if isPermanentConnectError(err) {
				return err
			}
			log.Printf("controller disconnected: %v", err)
			if c.cfg.OnControllerDisconnect == "stop" {
				c.manager.StopAll()
			}
			time.Sleep(time.Second)
			continue
		}
		return nil
	}
}

func isPermanentConnectError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errDuplicateAgentID) {
		return true
	}
	return status.Code(err) == codes.AlreadyExists
}

func (c *ControlClient) connectOnce(ctx context.Context) error {
	conn, err := grpc.NewClient(c.cfg.ControllerAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(grpcDialerWithVRF(c.cfg.ControllerVRF)),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(pb.JSONCodec{})),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pb.NewControlServiceClient(conn)
	stream, err := client.Connect(ctx)
	if err != nil {
		return err
	}
	log.Printf("controller connected: %s", c.cfg.ControllerAddr)
	advertiseUDP, err := ResolveAdvertiseUDP(c.cfg.ListenAddr, c.cfg.AdvertiseAddr)
	if err != nil {
		return err
	}
	if err := stream.Send(&pb.AgentMessage{Register: &pb.Register{AgentId: c.cfg.AgentID, Token: c.cfg.Token, UdpAddr: advertiseUDP}}); err != nil {
		return err
	}

	errCh := make(chan error, 2)
	go c.sendLoop(ctx, stream, errCh)
	go c.recvLoop(ctx, stream, errCh)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		if status.Code(err) == codes.AlreadyExists {
			return errors.Join(errDuplicateAgentID, err)
		}
		return err
	}
}

func (c *ControlClient) sendLoop(ctx context.Context, stream pb.ControlService_ConnectClient, errCh chan<- error) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			errCh <- ctx.Err()
			return
		case now := <-ticker.C:
			err := stream.Send(&pb.AgentMessage{Heartbeat: &pb.Heartbeat{
				AgentId:             c.cfg.AgentID,
				TsUnixNano:          now.UnixNano(),
				ActiveConfigVersion: c.manager.Version(),
				ActiveFlows:         c.manager.ActiveFlows(),
			}})
			if err != nil {
				errCh <- err
				return
			}
		case report := <-c.results:
			if report == nil {
				continue
			}
			if err := stream.Send(&pb.AgentMessage{ResultReport: report}); err != nil {
				errCh <- err
				return
			}
		}
	}
}

func (c *ControlClient) recvLoop(ctx context.Context, stream pb.ControlService_ConnectClient, errCh chan<- error) {
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				errCh <- nil
			} else {
				errCh <- err
			}
			return
		}
		if msg.ConfigSnapshot == nil {
			continue
		}
		snap := msg.ConfigSnapshot
		log.Printf("config received: version=%d hash=%s flows=%d", snap.ConfigVersion, snap.ConfigHash, len(snap.Flows))
		if c.receiver != nil {
			c.receiver.ApplyConfig(snap)
		}
		if err := c.manager.Apply(ctx, snap); err != nil {
			_ = stream.Send(&pb.AgentMessage{ConfigError: &pb.ConfigError{AgentId: c.cfg.AgentID, ConfigVersion: snap.ConfigVersion, Error: err.Error()}})
			continue
		}
		_ = stream.Send(&pb.AgentMessage{ConfigAck: &pb.ConfigAck{AgentId: c.cfg.AgentID, ConfigVersion: snap.ConfigVersion, ConfigHash: snap.ConfigHash, Status: "applied"}})
	}
}
