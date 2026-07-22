package agent

import (
	"context"
	"log"
	"net"
	"strings"
)

const defaultUDPReadBufferBytes = 4 << 20

func grpcDialerWithVRF(vrf string) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, address string) (net.Conn, error) {
		d := &net.Dialer{}
		if vrf != "" {
			d.Control = bindToDeviceControl(vrf)
		}
		return d.DialContext(ctx, "tcp", address)
	}
}

func udpDialerWithVRF(vrf string) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		d := &net.Dialer{}
		if vrf != "" {
			d.Control = bindToDeviceControl(vrf)
		}
		return d.DialContext(ctx, network, address)
	}
}

func listenPacketWithVRF(ctx context.Context, network, address, vrf string) (net.PacketConn, error) {
	lc := net.ListenConfig{}
	if vrf != "" {
		lc.Control = bindToDeviceControl(vrf)
	}
	conn, err := lc.ListenPacket(ctx, network, address)
	if err != nil {
		return nil, err
	}
	if strings.HasPrefix(network, "udp") {
		if err := setPacketConnReadBuffer(conn, defaultUDPReadBufferBytes); err != nil {
			log.Printf("udp receive buffer setup failed network=%s addr=%s err=%v", network, address, err)
		}
	}
	return conn, nil
}

func setPacketConnReadBuffer(conn net.PacketConn, size int) error {
	if conn == nil || size <= 0 {
		return nil
	}
	type readBufferSetter interface {
		SetReadBuffer(bytes int) error
	}
	if setter, ok := conn.(readBufferSetter); ok {
		return setter.SetReadBuffer(size)
	}
	return nil
}
