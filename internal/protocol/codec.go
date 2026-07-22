package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

func Encode(p Packet, size uint32) ([]byte, error) {
	if size == 0 {
		size = 64
	}
	p.Magic = Magic
	p.Version = Version
	if p.Type == 0 {
		p.Type = TypeRequest
	}
	if p.Type != TypeRequest && p.Type != TypeResponse {
		return nil, fmt.Errorf("bad packet type: %d", p.Type)
	}
	need, err := EncodedLen(p)
	if err != nil {
		return nil, err
	}
	if need > int(size) {
		return nil, fmt.Errorf("packet too small: need %d bytes, size=%d", need, size)
	}
	out := make([]byte, int(size))
	copy(out[0:4], []byte(Magic))
	out[4] = Version
	out[5] = p.Type
	off := 6
	binary.BigEndian.PutUint32(out[off:off+4], p.SessionID)
	off += 4
	binary.BigEndian.PutUint64(out[off:off+8], p.Seq)
	off += 8
	binary.BigEndian.PutUint64(out[off:off+8], uint64(p.SenderTxTimeNS))
	off += 8
	binary.BigEndian.PutUint64(out[off:off+8], uint64(p.ReflectorRxNS))
	off += 8
	binary.BigEndian.PutUint64(out[off:off+8], uint64(p.ReflectorTxNS))
	off += 8
	binary.BigEndian.PutUint32(out[off:off+4], p.IntervalMs)
	off += 4
	binary.BigEndian.PutUint32(out[off:off+4], p.FlowKey)
	return out, nil
}

func Decode(b []byte) (Packet, error) {
	var p Packet
	need, err := EncodedLen(p)
	if err != nil {
		return p, err
	}
	if len(b) < need {
		return p, errors.New("packet too short")
	}
	p.Magic = string(b[0:4])
	if p.Magic != Magic {
		return p, fmt.Errorf("bad magic: %q", p.Magic)
	}
	p.Version = b[4]
	if p.Version != Version {
		return p, fmt.Errorf("bad version: %d", p.Version)
	}
	p.Type = b[5]
	if p.Type != TypeRequest && p.Type != TypeResponse {
		return p, fmt.Errorf("bad packet type: %d", p.Type)
	}
	off := 6
	p.SessionID = binary.BigEndian.Uint32(b[off : off+4])
	off += 4
	p.Seq = binary.BigEndian.Uint64(b[off : off+8])
	off += 8
	p.SenderTxTimeNS = int64(binary.BigEndian.Uint64(b[off : off+8]))
	off += 8
	p.ReflectorRxNS = int64(binary.BigEndian.Uint64(b[off : off+8]))
	off += 8
	p.ReflectorTxNS = int64(binary.BigEndian.Uint64(b[off : off+8]))
	off += 8
	p.IntervalMs = binary.BigEndian.Uint32(b[off : off+4])
	off += 4
	p.FlowKey = binary.BigEndian.Uint32(b[off : off+4])
	off += 4
	p.Payload = b[off:]
	return p, nil
}
