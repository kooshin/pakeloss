package agent

import (
	"fmt"
	"net"
	"sort"
)

var listInterfaceAddrs = net.InterfaceAddrs

func ResolveAdvertiseUDP(listenAddr, advertiseAddr string) (string, error) {
	if advertiseAddr != "" {
		return validateAdvertiseUDP(advertiseAddr)
	}

	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "", fmt.Errorf("parse listen_addr %q: %w", listenAddr, err)
	}
	if !isWildcardHost(host) {
		return net.JoinHostPort(host, port), nil
	}

	candidates, err := discoverAdvertiseIPv4s()
	if err != nil {
		return "", err
	}
	if len(candidates) != 1 {
		return "", fmt.Errorf("listen_addr %q requires advertise_addr because detected %d candidate IPv4 addresses: %v", listenAddr, len(candidates), candidates)
	}
	return net.JoinHostPort(candidates[0], port), nil
}

func validateAdvertiseUDP(value string) (string, error) {
	host, port, err := net.SplitHostPort(value)
	if err != nil {
		return "", fmt.Errorf("parse advertise_addr %q: %w", value, err)
	}
	if host == "" || isWildcardHost(host) {
		return "", fmt.Errorf("advertise_addr %q must use a concrete host address", value)
	}
	if port == "" {
		return "", fmt.Errorf("advertise_addr %q must include a port", value)
	}
	return net.JoinHostPort(host, port), nil
}

func isWildcardHost(host string) bool {
	switch host {
	case "", "0.0.0.0", "::":
		return true
	default:
		return false
	}
}

func discoverAdvertiseIPv4s() ([]string, error) {
	addrs, err := listInterfaceAddrs()
	if err != nil {
		return nil, fmt.Errorf("list interface addrs: %w", err)
	}

	seen := map[string]struct{}{}
	out := make([]string, 0, len(addrs))
	for _, addr := range addrs {
		var ip net.IP
		switch v := addr.(type) {
		case *net.IPNet:
			ip = v.IP
		case *net.IPAddr:
			ip = v.IP
		}
		if ip == nil || ip.IsLoopback() {
			continue
		}
		ip = ip.To4()
		if ip == nil {
			continue
		}
		key := ip.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, key)
	}
	sort.Strings(out)
	return out, nil
}
