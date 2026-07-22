package controller

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"pakeloss/internal/model"
	"pakeloss/internal/pb"
)

type Server struct {
	pb.ControlServiceServer
	cfg              model.ControllerConfig
	configs          *ConfigStore
	runtime          *RuntimeStore
	results          *ResultStore
	mu               sync.Mutex
	agents           map[string]chan *pb.ControllerMessage
	grpc             *grpc.Server
	currentSessionID string
	lastSessionDate  string
	lastSessionSeq   int
	now              func() time.Time
}

func NewServer(cfg model.ControllerConfig, mesh model.MeshConfig, resultStore *ResultStore) *Server {
	factor := cfg.ReportBucketFactor
	if factor == 0 {
		factor = 10
	}
	mesh.ReportBucketFactor = factor
	if cfg.OutageThresholdMs != 0 {
		mesh.OutageThresholdMs = cfg.OutageThresholdMs
	}
	if mesh.OutageThresholdMs == 0 {
		mesh.OutageThresholdMs = 100
	}
	nowFn := time.Now
	if resultStore != nil && resultStore.now != nil {
		nowFn = resultStore.now
	}
	srv := &Server{
		cfg: cfg, configs: NewConfigStore(mesh), runtime: NewRuntimeStore(mesh),
		results: resultStore, agents: map[string]chan *pb.ControllerMessage{}, now: nowFn,
	}
	srv.runtime.SetReportFinalizeDelay(cfg.ReportFinalizeDelayDuration())
	if srv.results != nil {
		srv.results.SetReportFinalizeDelay(cfg.ReportFinalizeDelayDuration())
	}
	if id, ok, err := recoverLatestSessionID(cfg.ResultCSV, cfg.ResultJSONL, cfg.OutageEventCSV, cfg.OutageEventJSONL); err != nil {
		log.Printf("session id recovery failed: %v", err)
	} else if ok {
		date, seq, err := decodeSessionID(id)
		if err != nil {
			log.Printf("session id decode failed: %v", err)
		} else {
			srv.lastSessionDate = date
			srv.lastSessionSeq = seq
		}
	}
	srv.ensureLoggingForCurrentMesh(srv.configs.Mesh())
	return srv
}

func (s *Server) Runtime() *RuntimeStore { return s.runtime }

func (s *Server) CurrentSessionID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.currentSessionID
}

func (s *Server) RunRuntimeMonitor(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			s.writeRuntimeOutputs(s.runtime.FinalizeDueReports(now))
			s.writeRuntimeOutputs(s.runtime.ApplyControllerAgentLoss(now, 3*time.Second))
		}
	}
}

func (s *Server) ListenAndServeGRPC(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.cfg.GRPCAddr)
	if err != nil {
		return err
	}
	g := grpc.NewServer(grpc.ForceServerCodec(pb.JSONCodec{}))
	s.grpc = g
	pb.RegisterControlServiceServer(g, s)
	go func() {
		<-ctx.Done()
		g.GracefulStop()
	}()
	log.Printf("controller grpc listening: %s", s.cfg.GRPCAddr)
	return g.Serve(lis)
}

func (s *Server) Connect(stream pb.ControlService_ConnectServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.Register == nil {
		return io.ErrUnexpectedEOF
	}
	reg := first.Register
	if s.cfg.Token != "" && !secureTokenEqual(reg.Token, s.cfg.Token) {
		return status.Error(codes.Unauthenticated, "invalid controller token")
	}
	sendCh := make(chan *pb.ControllerMessage, 16)
	s.mu.Lock()
	if _, exists := s.agents[reg.AgentId]; exists {
		s.mu.Unlock()
		return status.Error(codes.AlreadyExists, fmt.Sprintf("agent_id already connected: %s", reg.AgentId))
	}
	s.agents[reg.AgentId] = sendCh
	s.mu.Unlock()
	mesh, changed, err := s.configs.UpsertDiscoveredNode(reg.AgentId, reg.UdpAddr)
	if err != nil {
		s.mu.Lock()
		delete(s.agents, reg.AgentId)
		close(sendCh)
		s.mu.Unlock()
		return err
	}
	if changed {
		s.runtime.SetDesiredConfigVersion(mesh)
		s.ensureLoggingForCurrentMesh(mesh)
	}
	s.runtime.AgentOnline(reg.AgentId, reg.UdpAddr, mesh.ConfigVersion)
	log.Printf("agent connected: %s", reg.AgentId)
	if changed {
		s.BroadcastSnapshots()
	} else {
		_ = s.SendSnapshot(reg.AgentId)
	}

	done := make(chan error, 2)
	go func() {
		for msg := range sendCh {
			if err := stream.Send(msg); err != nil {
				done <- err
				return
			}
		}
	}()
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				done <- err
				return
			}
			s.handleAgentMessage(msg)
		}
	}()
	err = <-done
	s.mu.Lock()
	if s.agents[reg.AgentId] == sendCh {
		delete(s.agents, reg.AgentId)
	}
	close(sendCh)
	s.mu.Unlock()
	s.runtime.AgentOffline(reg.AgentId)
	log.Printf("agent disconnected: %s err=%v", reg.AgentId, err)
	return err
}

func (s *Server) handleAgentMessage(msg *pb.AgentMessage) {
	switch {
	case msg.Heartbeat != nil:
		s.runtime.Heartbeat(msg.Heartbeat)
	case msg.ConfigAck != nil:
		log.Printf("config ack: agent=%s version=%d", msg.ConfigAck.AgentId, msg.ConfigAck.ConfigVersion)
		s.runtime.ConfigAck(msg.ConfigAck)
	case msg.ConfigError != nil:
		log.Printf("config error: agent=%s err=%s", msg.ConfigError.AgentId, msg.ConfigError.Error)
		s.runtime.ConfigError(msg.ConfigError)
	case msg.ResultReport != nil:
		s.writeRuntimeOutputs(s.runtime.IngestReport(msg.ResultReport))
	case msg.AgentLog != nil:
		log.Printf("agent log: %s %s", msg.AgentLog.AgentId, msg.AgentLog.Message)
	}
}

func (s *Server) writeRuntimeOutputs(summaries []*pb.ResultSummary) {
	if s.results != nil {
		for _, applied := range summaries {
			if err := s.results.Write(applied); err != nil {
				log.Printf("result write failed: %v", err)
			}
		}
	}
	if s.results != nil {
		for _, record := range s.runtime.DrainDebugRecords() {
			if err := s.results.WriteDebug(record); err != nil {
				log.Printf("result debug write failed: %v", err)
			}
		}
		for _, record := range s.runtime.DrainOutageEventRecords() {
			if err := s.results.WriteOutageEvent(record); err != nil {
				log.Printf("outage event write failed: %v", err)
			}
		}
	}
}

func (s *Server) CloseActiveOutages(reason string) {
	now := s.now()
	s.runtime.CloseActiveOutages(now, reason)
	s.writeRuntimeOutputs(nil)
}

func (s *Server) SendSnapshot(agentID string) error {
	snap, err := CompileSnapshot(s.configs.Mesh(), agentID)
	if err != nil {
		return err
	}
	s.mu.Lock()
	ch := s.agents[agentID]
	s.mu.Unlock()
	if ch == nil {
		return nil
	}
	ch <- &pb.ControllerMessage{ConfigSnapshot: snap}
	log.Printf("config sent: agent=%s version=%d flows=%d", agentID, snap.ConfigVersion, len(snap.Flows))
	return nil
}

func (s *Server) BroadcastSnapshots() {
	s.mu.Lock()
	agentIDs := make([]string, 0, len(s.agents))
	for agentID := range s.agents {
		agentIDs = append(agentIDs, agentID)
	}
	s.mu.Unlock()
	for _, agentID := range agentIDs {
		_ = s.SendSnapshot(agentID)
	}
}

func (s *Server) SetFlowState(flowID, state string) error {
	before := s.configs.Mesh()
	mesh, err := s.configs.SetFlowState(flowID, state)
	if err != nil {
		return err
	}
	s.runtime.SetDesiredConfigVersion(mesh)
	s.syncResultLoggingTransition(before, mesh)
	var src string
	for _, f := range mesh.Flows {
		if f.ID == flowID {
			src = f.Src
			break
		}
	}
	if src != "" {
		return s.SendSnapshot(src)
	}
	return nil
}

func (s *Server) SetAllFlowStates(state string) {
	before := s.configs.Mesh()
	mesh := s.configs.SetAllFlowStates(state)
	s.runtime.SetDesiredConfigVersion(mesh)
	s.syncResultLoggingTransition(before, mesh)
	seen := map[string]bool{}
	for _, f := range mesh.Flows {
		if f.Src == "" || seen[f.Src] {
			continue
		}
		seen[f.Src] = true
		_ = s.SendSnapshot(f.Src)
	}
}

func (s *Server) SetAgentEnabled(agentID string, enabled bool) error {
	before := s.configs.Mesh()
	mesh, err := s.configs.SetAgentEnabled(agentID, enabled)
	if err != nil {
		return err
	}
	s.runtime.SetDesiredConfigVersion(mesh)
	s.syncResultLoggingTransition(before, mesh)
	s.BroadcastSnapshots()
	return nil
}

func (s *Server) RestartAllFlows() {
	before := s.configs.Mesh()
	if meshHasRunningFlow(before) {
		s.SetAllFlowStates("stopped")
	}
	s.SetAllFlowStates("running")
}

func (s *Server) ensureLoggingForCurrentMesh(mesh model.MeshConfig) {
	if !meshHasRunningFlow(mesh) {
		return
	}
	if s.CurrentSessionID() != "" {
		return
	}
	sessionID, err := s.allocateNextSessionID()
	if err != nil {
		log.Printf("result session id allocation failed: %v", err)
		return
	}
	if s.results == nil {
		return
	}
	if err := s.results.StartSession(sessionID); err != nil {
		log.Printf("result session sync failed: %v", err)
	}
}

func (s *Server) syncResultLoggingTransition(before, after model.MeshConfig) {
	beforeRunning := meshHasRunningFlow(before)
	afterRunning := meshHasRunningFlow(after)
	switch {
	case beforeRunning && !afterRunning:
		if s.results == nil {
			s.clearCurrentSessionID()
			return
		}
		s.CloseActiveOutages("session_stopped")
		if err := s.results.StopSessionWithSummary(s.runtime.Flows(), s.runtime.UnmeasurableEvents(s.now())); err != nil {
			log.Printf("result session sync failed: %v", err)
		}
		s.clearCurrentSessionID()
	case !beforeRunning && afterRunning:
		sessionID, err := s.allocateNextSessionID()
		if err != nil {
			log.Printf("result session id allocation failed: %v", err)
			return
		}
		if s.results == nil {
			return
		}
		if err := s.results.StartSession(sessionID); err != nil {
			log.Printf("result session sync failed: %v", err)
		}
	}
}

func (s *Server) allocateNextSessionID() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	date := sessionDateFromTime(s.now())
	seq := 1
	if date == s.lastSessionDate {
		seq = s.lastSessionSeq + 1
	}
	sessionID, err := encodeSessionID(date, seq)
	if err != nil {
		return "", err
	}
	s.lastSessionDate = date
	s.lastSessionSeq = seq
	s.currentSessionID = sessionID
	return s.currentSessionID, nil
}

func (s *Server) clearCurrentSessionID() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.currentSessionID = ""
}

func meshHasRunningFlow(mesh model.MeshConfig) bool {
	for _, flow := range mesh.Flows {
		if flow.State == "running" {
			return true
		}
	}
	return false
}
