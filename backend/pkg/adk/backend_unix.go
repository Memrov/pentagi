//go:build unix

package adk

import (
	"errors"
	"os/exec"
	"syscall"
)

// setPgid puts the command in its own process group and replaces
// CommandContext's default Cancel — which kills only the direct child — with a
// group kill, so `sh -c` pipelines and background children die with the shell.
func setPgid(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error {
		if cmd.Process == nil {
			return nil
		}
		if err := syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
			return err
		}
		return nil
	}
}
