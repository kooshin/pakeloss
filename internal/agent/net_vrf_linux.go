//go:build linux

package agent

import (
	"fmt"
	"syscall"

	"golang.org/x/sys/unix"
)

func bindToDeviceControl(vrf string) func(network, address string, c syscall.RawConn) error {
	return func(network, address string, c syscall.RawConn) error {
		var controlErr error
		if err := c.Control(func(fd uintptr) {
			controlErr = unix.SetsockoptString(int(fd), unix.SOL_SOCKET, unix.SO_BINDTODEVICE, vrf)
		}); err != nil {
			return err
		}
		if controlErr != nil {
			return fmt.Errorf("bind to vrf %q: %w", vrf, controlErr)
		}
		return nil
	}
}
