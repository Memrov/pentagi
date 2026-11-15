//go:build !unix

package adk

import "os/exec"

// There are no unix process groups to kill here; CommandContext's default
// child kill stands and grandchildren may survive cancellation.
func setPgid(*exec.Cmd) {}
