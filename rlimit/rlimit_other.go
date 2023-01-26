// +build !freebsd

package rlimit

import (
	"fmt"
	"log"
	"syscall"
)

func SetRLimit(required uint64) error {
	var rLimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
		return err
	}
	if rLimit.Cur < required {
		log.Printf("Setting open files rlimit of %d to %d.", rLimit.Cur, required)
		rLimit.Cur = required
		if err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
			return err
		}
		if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit); err != nil {
			return err
		}
		if rLimit.Cur < required {
			return fmt.Errorf("Could not change open files rlimit to: %d", required)
		}
	}
	return nil
}
