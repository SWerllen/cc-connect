//go:build windows

package codex

import "syscall"

const processQueryLimitedInformation = 0x1000

func sandboxedTextOwnerProcessAlive(pid int) bool {
	handle, err := syscall.OpenProcess(processQueryLimitedInformation, false, uint32(pid))
	if err != nil {
		return err == syscall.ERROR_ACCESS_DENIED
	}
	_ = syscall.CloseHandle(handle)
	return true
}
