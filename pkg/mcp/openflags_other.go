//go:build !unix

package mcp

const (
	nonBlockingOpenFlag = 0
	noFollowOpenFlag    = 0
)
