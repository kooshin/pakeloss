package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"

	"pakeloss/internal/model"
	"pakeloss/internal/pb"
	"pakeloss/internal/protocol"
)

func CompileSnapshot(mesh model.MeshConfig, agentID string) (*pb.ConfigSnapshot, error) {
	flowKeys, err := compileFlowKeys(mesh)
	if err != nil {
		return nil, err
	}
	nodeAddr := map[string]string{}
	for _, n := range mesh.Nodes {
		nodeAddr[n.ID] = n.UDPAddr
	}
	flows := []*pb.FlowConfig{}
	for _, f := range mesh.Flows {
		if f.Src != agentID && f.Dst != agentID {
			continue
		}
		dstAddr := nodeAddr[f.Dst]
		if dstAddr == "" {
			return nil, fmt.Errorf("destination node not found: %s", f.Dst)
		}
		flowKey := flowKeys[f.ID]
		if _, err := protocol.PayloadSizeFromTotalIPPacketSize(protocol.Packet{
			FlowKey:    flowKey,
			IntervalMs: f.IntervalMs,
		}, f.PacketSize); err != nil {
			return nil, fmt.Errorf("invalid packet size for flow %s: %w", f.ID, err)
		}
		factor := mesh.ReportBucketFactor
		if factor == 0 {
			factor = 10
		}
		interval := f.IntervalMs
		if interval == 0 {
			interval = 10
		}
		if uint64(interval)*uint64(factor) > uint64(^uint32(0)) {
			return nil, fmt.Errorf("report window overflows for flow %s", f.ID)
		}
		flows = append(flows, &pb.FlowConfig{
			Id:                  f.ID,
			SrcId:               f.Src,
			FlowKey:             flowKey,
			DstId:               f.Dst,
			DstAddr:             dstAddr,
			IntervalMs:          f.IntervalMs,
			PacketSize:          f.PacketSize,
			SourcePortCount:     f.SourcePortCount,
			LossConfirmWindowMs: f.LossConfirmWindowMs,
			ReportWindowMs:      interval * factor,
			State:               f.State,
		})
	}
	snap := &pb.ConfigSnapshot{AgentId: agentID, ConfigVersion: mesh.ConfigVersion, Flows: flows}
	b, _ := json.Marshal(snap)
	sum := sha256.Sum256(b)
	snap.ConfigHash = "sha256:" + hex.EncodeToString(sum[:])
	return snap, nil
}

func compileFlowKeys(mesh model.MeshConfig) (map[string]uint32, error) {
	keys := make(map[string]uint32, len(mesh.Flows))
	seen := make(map[uint32]string, len(mesh.Flows))
	for _, f := range mesh.Flows {
		key := protocol.ComputeFlowKey(f.Src, f.Dst, f.ID)
		signature := f.Src + "\x00" + f.Dst + "\x00" + f.ID
		if existing, ok := seen[key]; ok && existing != signature {
			return nil, fmt.Errorf("flow key collision: %s and %s", existing, signature)
		}
		seen[key] = signature
		keys[f.ID] = key
	}
	return keys, nil
}
