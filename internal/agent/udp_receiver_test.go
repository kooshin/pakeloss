package agent

import (
	"errors"
	"net"
	"testing"
	"time"

	"pakeloss/internal/pb"
	"pakeloss/internal/protocol"
)

func TestReceiverReflectsRequest(t *testing.T) {
	r := NewUDPReceiver("node-b", "127.0.0.1:0", nil)
	r.ApplyConfig(&pb.ConfigSnapshot{Flows: []*pb.FlowConfig{{
		Id:         "node-a<=>node-b",
		SrcId:      "node-a",
		DstId:      "node-b",
		FlowKey:    1,
		IntervalMs: 10,
		PacketSize: 96,
		State:      "running",
	}}})
	server := &fakePacketConn{}
	clientAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50000}

	req := protocol.Packet{Type: protocol.TypeRequest, FlowKey: 1, Seq: 7, SenderTxTimeNS: 123, SessionID: 99, IntervalMs: 10}
	if err := r.reflectPacket(server, clientAddr, req, time.Unix(0, 456)); err != nil {
		t.Fatal(err)
	}
	resp, err := protocol.Decode(server.lastWrite)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Type != protocol.TypeResponse || resp.FlowKey != req.FlowKey || resp.Seq != req.Seq || resp.SenderTxTimeNS != req.SenderTxTimeNS || resp.SessionID != req.SessionID || resp.IntervalMs != req.IntervalMs {
		t.Fatalf("response did not preserve request fields: %+v", resp)
	}
	if resp.ReflectorRxNS != 456 || resp.ReflectorTxNS == 0 {
		t.Fatalf("response timestamps = %+v", resp)
	}
}

func TestReceiverIgnoresUnknownFlow(t *testing.T) {
	r := NewUDPReceiver("node-b", "127.0.0.1:0", nil)
	server := &fakePacketConn{}
	clientAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50000}

	req := protocol.Packet{Type: protocol.TypeRequest, FlowKey: 404, Seq: 1, SenderTxTimeNS: 123, SessionID: 99, IntervalMs: 10}
	if err := r.reflectPacket(server, clientAddr, req, time.Unix(0, 456)); err != nil {
		t.Fatal(err)
	}
	if len(server.lastWrite) != 0 {
		t.Fatalf("unexpected response len=%d", len(server.lastWrite))
	}
}

func TestReceiverHandleRequestRecordsAndReflects(t *testing.T) {
	r := NewUDPReceiver("node-b", "127.0.0.1:0", nil)
	r.ApplyConfig(&pb.ConfigSnapshot{Flows: []*pb.FlowConfig{{
		Id:         "node-a->node-b",
		SrcId:      "node-a",
		DstId:      "node-b",
		FlowKey:    1,
		IntervalMs: 10,
		PacketSize: 96,
		State:      "running",
	}}})
	server := &fakePacketConn{}
	clientAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 50000}
	now := time.Date(2026, time.July, 6, 0, 51, 21, 50*int(time.Millisecond), time.UTC)
	req := protocol.Packet{Type: protocol.TypeRequest, FlowKey: 1, Seq: 7, SenderTxTimeNS: now.UnixNano(), SessionID: 99, IntervalMs: 10}

	if err := r.handleRequestPacket(server, clientAddr, req, clientAddr.String(), now); err != nil {
		t.Fatal(err)
	}
	reports := r.snapshot(now.Add(100 * time.Millisecond))
	if len(reports) != 1 || len(reports[0].SeqRanges) != 1 || reports[0].SeqRanges[0].Start != 7 || reports[0].SeqRanges[0].End != 7 {
		t.Fatalf("reports = %+v, want one finalized rx report", reports)
	}
	if len(server.lastWrite) == 0 {
		t.Fatal("expected reflected response to be written")
	}
}

func TestReceiverHandleRequestKeepsCountWhenReflectFails(t *testing.T) {
	r := NewUDPReceiver("node-b", "127.0.0.1:0", nil)
	r.ApplyConfig(&pb.ConfigSnapshot{Flows: []*pb.FlowConfig{{
		Id:         "node-a->node-b",
		SrcId:      "node-a",
		DstId:      "node-b",
		FlowKey:    1,
		IntervalMs: 10,
		PacketSize: 96,
		State:      "running",
	}}})
	server := &fakePacketConn{writeErr: errors.New("boom")}
	now := time.Date(2026, time.July, 6, 0, 51, 21, 50*int(time.Millisecond), time.UTC)
	req := protocol.Packet{Type: protocol.TypeRequest, FlowKey: 1, Seq: 7, SenderTxTimeNS: now.UnixNano(), SessionID: 99, IntervalMs: 10}

	if err := r.handleRequestPacket(server, &net.UDPAddr{}, req, "", now); err == nil {
		t.Fatal("expected reflect error")
	}
	reports := r.snapshot(now.Add(100 * time.Millisecond))
	if len(reports) != 1 || len(reports[0].SeqRanges) != 1 {
		t.Fatalf("reports = %+v, want count preserved despite reflect error", reports)
	}
}

func TestReceiverSnapshotSkipsCurrentBucket(t *testing.T) {
	r := NewUDPReceiver("node-b", "127.0.0.1:0", nil)
	r.ApplyConfig(&pb.ConfigSnapshot{Flows: []*pb.FlowConfig{{
		Id:         "node-a->node-b",
		SrcId:      "node-a",
		DstId:      "node-b",
		FlowKey:    1,
		IntervalMs: 10,
		PacketSize: 96,
		State:      "running",
	}}})
	base := time.Date(2026, time.July, 6, 0, 51, 21, 0, time.UTC)
	r.recordFromAt(protocol.Packet{FlowKey: 1, SenderTxTimeNS: base.Add(50 * time.Millisecond).UnixNano(), IntervalMs: 10}, "", base.Add(50*time.Millisecond))
	r.recordFromAt(protocol.Packet{FlowKey: 1, SenderTxTimeNS: base.Add(150 * time.Millisecond).UnixNano(), IntervalMs: 10}, "", base.Add(150*time.Millisecond))

	if reports := r.snapshot(base.Add(150 * time.Millisecond)); len(reports) != 1 || len(reports[0].SeqRanges) != 1 {
		t.Fatalf("reports at current bucket = %+v, want only prior finalized bucket", reports)
	}
	if reports := r.snapshot(base.Add(250 * time.Millisecond)); len(reports) != 1 || len(reports[0].SeqRanges) != 1 {
		t.Fatalf("reports after next tick = %+v, want second finalized bucket", reports)
	}
}

func TestReceiverUsesLocalReceiveTimeForBucketing(t *testing.T) {
	r := NewUDPReceiver("node-b", "127.0.0.1:0", nil)
	r.ApplyConfig(&pb.ConfigSnapshot{Flows: []*pb.FlowConfig{{
		Id:         "node-a->node-b",
		SrcId:      "node-a",
		DstId:      "node-b",
		FlowKey:    1,
		IntervalMs: 10,
		PacketSize: 96,
		State:      "running",
	}}})
	base := time.Date(2026, time.July, 6, 0, 51, 21, 0, time.UTC)
	r.recordFromAt(protocol.Packet{FlowKey: 1, SenderTxTimeNS: base.Add(3 * time.Second).UnixNano(), IntervalMs: 10}, "", base.Add(50*time.Millisecond))
	r.recordFromAt(protocol.Packet{FlowKey: 1, SenderTxTimeNS: base.Add(-3 * time.Second).UnixNano(), IntervalMs: 10}, "", base.Add(150*time.Millisecond))

	if reports := r.snapshot(base.Add(150 * time.Millisecond)); len(reports) != 1 || len(reports[0].SeqRanges) != 1 || reports[0].Ts != "2026-07-06T00:51:21.1Z" {
		t.Fatalf("reports with future sender clock = %+v, want local first bucket", reports)
	}
	if reports := r.snapshot(base.Add(250 * time.Millisecond)); len(reports) != 1 || len(reports[0].SeqRanges) != 1 || reports[0].Ts != "2026-07-06T00:51:21.2Z" {
		t.Fatalf("reports with past sender clock = %+v, want local second bucket", reports)
	}
}

func TestReceiverEnqueueReflectDropsWhenQueueFull(t *testing.T) {
	r := NewUDPReceiver("node-b", "127.0.0.1:0", nil)
	r.reflects = make(chan receiverReflectRequest, 1)
	req := protocol.Packet{Type: protocol.TypeRequest, FlowKey: 1, Seq: 1, SenderTxTimeNS: 123, SessionID: 99, IntervalMs: 10}
	if err := r.enqueueReflect(&net.UDPAddr{}, req, time.Unix(0, 1)); err != nil {
		t.Fatalf("first enqueue failed: %v", err)
	}
	if err := r.enqueueReflect(&net.UDPAddr{}, req, time.Unix(0, 2)); !errors.Is(err, errReflectQueueFull) {
		t.Fatalf("second enqueue err = %v, want %v", err, errReflectQueueFull)
	}
}

type fakePacketConn struct {
	lastWrite []byte
	lastAddr  net.Addr
	writeErr  error
}

func (c *fakePacketConn) ReadFrom(_ []byte) (int, net.Addr, error) { return 0, nil, nil }
func (c *fakePacketConn) WriteTo(b []byte, addr net.Addr) (int, error) {
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	c.lastWrite = append([]byte(nil), b...)
	c.lastAddr = addr
	return len(b), nil
}
func (c *fakePacketConn) Close() error                     { return nil }
func (c *fakePacketConn) LocalAddr() net.Addr              { return &net.UDPAddr{} }
func (c *fakePacketConn) SetDeadline(time.Time) error      { return nil }
func (c *fakePacketConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fakePacketConn) SetWriteDeadline(time.Time) error { return nil }
