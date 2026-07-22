package protocol

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

const (
	Magic           = "PLAB"
	Version         = 1
	TypeRequest     = 1
	TypeResponse    = 2
	IPv4HeaderSize  = 20
	UDPHeaderSize   = 8
	IPUDPHeaderSize = IPv4HeaderSize + UDPHeaderSize
)

type Packet struct {
	Magic          string `json:"magic"`
	Version        uint8  `json:"version"`
	Type           uint8  `json:"type"`
	FlowKey        uint32 `json:"flow_key"`
	Seq            uint64 `json:"seq"`
	SenderTxTimeNS int64  `json:"sender_tx_time_ns"`
	ReflectorRxNS  int64  `json:"reflector_rx_ns"`
	ReflectorTxNS  int64  `json:"reflector_tx_ns"`
	SessionID      uint32 `json:"session_id"`
	IntervalMs     uint32 `json:"interval_ms"`
	Payload        []byte `json:"payload,omitempty"`
}

func EncodedLen(p Packet) (int, error) {
	_ = p
	return 4 + 1 + 1 + 4 + 8 + 8 + 8 + 8 + 4 + 4, nil
}

func PayloadSizeFromTotalIPPacketSize(p Packet, totalSize uint32) (uint32, error) {
	encodedLen, err := EncodedLen(p)
	if err != nil {
		return 0, err
	}
	if totalSize < IPUDPHeaderSize {
		return 0, fmt.Errorf("packet too small: need at least %d bytes total, size=%d", IPUDPHeaderSize, totalSize)
	}
	payloadSize := totalSize - IPUDPHeaderSize
	if encodedLen > int(payloadSize) {
		return 0, fmt.Errorf("packet too small: need %d bytes total, size=%d", encodedLen+IPUDPHeaderSize, totalSize)
	}
	return payloadSize, nil
}

func ComputeFlowKey(srcID, dstID, flowID string) uint32 {
	sum := sha256.Sum256([]byte(srcID + "\x00" + dstID + "\x00" + flowID))
	return binary.BigEndian.Uint32(sum[:4])
}
