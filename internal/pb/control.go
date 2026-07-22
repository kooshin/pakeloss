package pb

import (
	"context"
	"encoding/json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/encoding"
)

func init() {
	encoding.RegisterCodec(JSONCodec{})
}

type JSONCodec struct{}

func (JSONCodec) Marshal(v any) ([]byte, error)   { return json.Marshal(v) }
func (JSONCodec) Unmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
func (JSONCodec) Name() string                    { return "json" }

type AgentMessage struct {
	Register     *Register     `json:"register,omitempty"`
	Heartbeat    *Heartbeat    `json:"heartbeat,omitempty"`
	ConfigAck    *ConfigAck    `json:"config_ack,omitempty"`
	ConfigError  *ConfigError  `json:"config_error,omitempty"`
	AgentLog     *AgentLog     `json:"agent_log,omitempty"`
	ResultReport *ResultReport `json:"result_report,omitempty"`
}

type ControllerMessage struct {
	ConfigSnapshot *ConfigSnapshot `json:"config_snapshot,omitempty"`
}

type Register struct {
	AgentId string `json:"agent_id"`
	Token   string `json:"token"`
	UdpAddr string `json:"udp_addr"`
}

type Heartbeat struct {
	AgentId             string `json:"agent_id"`
	TsUnixNano          int64  `json:"ts_unix_nano"`
	ActiveConfigVersion uint64 `json:"active_config_version"`
	ActiveFlows         uint32 `json:"active_flows"`
}

type ConfigSnapshot struct {
	AgentId       string        `json:"agent_id"`
	ConfigVersion uint64        `json:"config_version"`
	ConfigHash    string        `json:"config_hash"`
	Flows         []*FlowConfig `json:"flows"`
}

type FlowConfig struct {
	Id                  string `json:"id"`
	SrcId               string `json:"src_id"`
	FlowKey             uint32 `json:"flow_key"`
	DstId               string `json:"dst_id"`
	DstAddr             string `json:"dst_addr"`
	IntervalMs          uint32 `json:"interval_ms"`
	PacketSize          uint32 `json:"packet_size"`
	SourcePortCount     uint32 `json:"source_port_count"`
	LossConfirmWindowMs uint32 `json:"loss_confirm_window_ms"`
	ReportWindowMs      uint32 `json:"report_window_ms"`
	State               string `json:"state"`
}

type ConfigAck struct {
	AgentId       string `json:"agent_id"`
	ConfigVersion uint64 `json:"config_version"`
	ConfigHash    string `json:"config_hash"`
	Status        string `json:"status"`
}

type ConfigError struct {
	AgentId       string `json:"agent_id"`
	ConfigVersion uint64 `json:"config_version"`
	Error         string `json:"error"`
}

type ResultSummary struct {
	Ts         string  `json:"ts"`
	AgentId    string  `json:"agent_id"`
	FlowKey    uint32  `json:"flow_key"`
	Src        string  `json:"src"`
	Dst        string  `json:"dst"`
	FlowId     string  `json:"flow_id"`
	IntervalMs uint32  `json:"interval_ms"`
	Tx         uint64  `json:"tx"`
	Rx         uint64  `json:"rx"`
	Lost       uint64  `json:"lost"`
	LossRatio  float64 `json:"loss_ratio"`
	Duplicate  uint64  `json:"duplicate"`
	Reorder    uint64  `json:"reorder"`
	OutageMs   uint64  `json:"outage_ms"`
}

type SeqRange struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
}

type ResultReport struct {
	Ts         string `json:"ts"`
	AgentId    string `json:"agent_id"`
	FlowKey    uint32 `json:"flow_key"`
	Src        string `json:"src"`
	Dst        string `json:"dst"`
	FlowId     string `json:"flow_id"`
	SessionId  uint32 `json:"session_id,omitempty"`
	IntervalMs uint32 `json:"interval_ms"`
	WindowMs   uint32 `json:"window_ms"`
	Role       string `json:"role"`
	// Legacy in-process counters are intentionally excluded from the wire format.
	// Sequence ranges are the source of truth used by the controller.
	Tx        uint64      `json:"-"`
	Rx        uint64      `json:"-"`
	Lost      uint64      `json:"-"`
	Duplicate uint64      `json:"duplicate"`
	Reorder   uint64      `json:"reorder"`
	SeqRanges []*SeqRange `json:"seq_ranges,omitempty"`
}

type AgentLog struct {
	AgentId string `json:"agent_id"`
	Level   string `json:"level"`
	Message string `json:"message"`
	Ts      string `json:"ts"`
}

type ControlServiceClient interface {
	Connect(ctx context.Context, opts ...grpc.CallOption) (ControlService_ConnectClient, error)
}

type controlServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewControlServiceClient(cc grpc.ClientConnInterface) ControlServiceClient {
	return &controlServiceClient{cc: cc}
}

func (c *controlServiceClient) Connect(ctx context.Context, opts ...grpc.CallOption) (ControlService_ConnectClient, error) {
	stream, err := c.cc.NewStream(ctx, &ControlService_ServiceDesc.Streams[0], "/probe.control.ControlService/Connect", opts...)
	if err != nil {
		return nil, err
	}
	return &controlServiceConnectClient{ClientStream: stream}, nil
}

type ControlService_ConnectClient interface {
	Send(*AgentMessage) error
	Recv() (*ControllerMessage, error)
	grpc.ClientStream
}

type controlServiceConnectClient struct {
	grpc.ClientStream
}

func (x *controlServiceConnectClient) Send(m *AgentMessage) error {
	return x.ClientStream.SendMsg(m)
}

func (x *controlServiceConnectClient) Recv() (*ControllerMessage, error) {
	m := new(ControllerMessage)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

type ControlServiceServer interface {
	Connect(ControlService_ConnectServer) error
}

type ControlService_ConnectServer interface {
	Send(*ControllerMessage) error
	Recv() (*AgentMessage, error)
	grpc.ServerStream
}

func RegisterControlServiceServer(s grpc.ServiceRegistrar, srv ControlServiceServer) {
	s.RegisterService(&ControlService_ServiceDesc, srv)
}

func _ControlService_Connect_Handler(srv any, stream grpc.ServerStream) error {
	return srv.(ControlServiceServer).Connect(&controlServiceConnectServer{ServerStream: stream})
}

type controlServiceConnectServer struct {
	grpc.ServerStream
}

func (x *controlServiceConnectServer) Send(m *ControllerMessage) error {
	return x.ServerStream.SendMsg(m)
}

func (x *controlServiceConnectServer) Recv() (*AgentMessage, error) {
	m := new(AgentMessage)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

var ControlService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "probe.control.ControlService",
	HandlerType: (*ControlServiceServer)(nil),
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Connect",
			Handler:       _ControlService_Connect_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
	Metadata: "proto/control.proto",
}
