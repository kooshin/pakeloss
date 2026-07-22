//go:build !linux

package agent

import (
	"fmt"
	"syscall"
)

func bindToDeviceControl(vrf string) func(network, address string, c syscall.RawConn) error {
	return func(network, address string, c syscall.RawConn) error {
		return fmt.Errorf("vrf binding is not supported on this platform: %s", vrf)
	}
}
