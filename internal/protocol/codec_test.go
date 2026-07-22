package protocol

import "testing"

func TestEncodeDecode(t *testing.T) {
	in := Packet{Type: TypeRequest, FlowKey: 0x12345678, Seq: 7, SenderTxTimeNS: 123, SessionID: 99, IntervalMs: 10}
	payloadSize, err := PayloadSizeFromTotalIPPacketSize(in, 96)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Encode(in, payloadSize)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if out.Type != in.Type || out.FlowKey != in.FlowKey || out.Seq != in.Seq || out.SenderTxTimeNS != in.SenderTxTimeNS || out.SessionID != in.SessionID || out.IntervalMs != in.IntervalMs {
		t.Fatalf("decoded mismatch: %+v", out)
	}
}

func TestEncodeDecodeResponseTimestamps(t *testing.T) {
	in := Packet{Type: TypeResponse, FlowKey: 1, Seq: 2, SenderTxTimeNS: 3, ReflectorRxNS: 4, ReflectorTxNS: 5, SessionID: 6, IntervalMs: 10}
	payloadSize, err := PayloadSizeFromTotalIPPacketSize(in, 96)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Encode(in, payloadSize)
	if err != nil {
		t.Fatal(err)
	}
	out, err := Decode(b)
	if err != nil {
		t.Fatal(err)
	}
	if out.Type != TypeResponse || out.SenderTxTimeNS != 3 || out.ReflectorRxNS != 4 || out.ReflectorTxNS != 5 {
		t.Fatalf("decoded response = %+v", out)
	}
}

func TestDecodeRejectsBadPacketType(t *testing.T) {
	in := Packet{Type: TypeRequest, FlowKey: 1, Seq: 1, IntervalMs: 10}
	payloadSize, err := PayloadSizeFromTotalIPPacketSize(in, 96)
	if err != nil {
		t.Fatal(err)
	}
	b, err := Encode(in, payloadSize)
	if err != nil {
		t.Fatal(err)
	}
	b[5] = 99
	if _, err := Decode(b); err == nil {
		t.Fatal("expected bad packet type error")
	}
}

func TestPayloadSizeFromTotalIPPacketSizeRejectsTooSmallTotal(t *testing.T) {
	p := Packet{FlowKey: 1, IntervalMs: 10}
	if _, err := PayloadSizeFromTotalIPPacketSize(p, 32); err == nil {
		t.Fatal("expected too small total packet size error")
	}
}

func TestComputeFlowKeyStable(t *testing.T) {
	if got, want := ComputeFlowKey("node-a", "node-b", "flow-a"), ComputeFlowKey("node-a", "node-b", "flow-a"); got != want {
		t.Fatalf("flow key not stable: got=%d want=%d", got, want)
	}
}
