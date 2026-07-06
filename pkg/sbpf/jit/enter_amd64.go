package jit

// enter transfers control to compiled code at ctx.Resume. Implemented
// in enter_amd64.s.
//
//go:noescape
func enter(ctx *ExecContext)
