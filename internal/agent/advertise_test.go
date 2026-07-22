package agent

import (
	"net"
	"strings"
	"testing"
)

func TestResolveAdvertiseUDPUsesExplicitOverride(t *testing.T) {
	got, err := ResolveAdvertiseUDP("0.0.0.0:40001", "192.0.2.10:40001")
	if err != nil {
		t.Fatal(err)
	}
	if got != "192.0.2.10:40001" {
		t.Fatalf("advertise udp = %q, want %q", got, "192.0.2.10:40001")
	}
}

func TestResolveAdvertiseUDPUsesConcreteListenHost(t *testing.T) {
	got, err := ResolveAdvertiseUDP("192.0.2.11:40001", "")
	if err != nil {
		t.Fatal(err)
	}
	if got != "192.0.2.11:40001" {
		t.Fatalf("advertise udp = %q, want %q", got, "192.0.2.11:40001")
	}
}

func TestResolveAdvertiseUDPAutoDetectsSingleIPv4(t *testing.T) {
	orig := listInterfaceAddrs
	listInterfaceAddrs = func() ([]net.Addr, error) {
		return []net.Addr{
			&net.IPNet{IP: net.ParseIP("127.0.0.1"), Mask: net.CIDRMask(8, 32)},
			&net.IPNet{IP: net.ParseIP("192.0.2.21"), Mask: net.CIDRMask(24, 32)},
		}, nil
	}
	defer func() { listInterfaceAddrs = orig }()

	got, err := ResolveAdvertiseUDP("0.0.0.0:40001", "")
	if err != nil {
		t.Fatal(err)
	}
	if got != "192.0.2.21:40001" {
		t.Fatalf("advertise udp = %q, want %q", got, "192.0.2.21:40001")
	}
}

func TestResolveAdvertiseUDPRejectsMultipleIPv4Candidates(t *testing.T) {
	orig := listInterfaceAddrs
	listInterfaceAddrs = func() ([]net.Addr, error) {
		return []net.Addr{
			&net.IPNet{IP: net.ParseIP("192.0.2.21"), Mask: net.CIDRMask(24, 32)},
			&net.IPNet{IP: net.ParseIP("198.51.100.21"), Mask: net.CIDRMask(24, 32)},
		}, nil
	}
	defer func() { listInterfaceAddrs = orig }()

	_, err := ResolveAdvertiseUDP("0.0.0.0:40001", "")
	if err == nil {
		t.Fatal("expected error for multiple candidates")
	}
	if !strings.Contains(err.Error(), "requires advertise_addr") {
		t.Fatalf("unexpected error: %v", err)
	}
}
