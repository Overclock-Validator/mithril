//go:build unix

package mcp

import "syscall"

const (
	nonBlockingOpenFlag = syscall.O_NONBLOCK
	noFollowOpenFlag    = syscall.O_NOFOLLOW
)
