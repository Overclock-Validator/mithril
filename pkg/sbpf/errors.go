package sbpf

import (
	"errors"
	"fmt"
)

// Exception codes.
var (
	ExcDivideByZero           = errors.New("divide by zero at BPF instruction")
	ExcDivideOverflow         = errors.New("divide overflow")
	ExcOutOfCU                = errors.New("compute unit overrun")
	ExcCallDepth              = errors.New("call depth exceeded")
	ExcInvalidInstr           = errors.New("invalid instruction - feature not enabled")
	ErrOutOfBounds            = errors.New("value out of bounds")
	ErrInvalidSectionHeader   = errors.New("invalid section header")
	ExcUnsupportedInstruction = errors.New("unsupported BPF instruction")
)

type ErrStringTooLong struct {
	Name string
	Len  uint64
}

func (e *ErrStringTooLong) Error() string {
	return fmt.Sprintf("Section or symbol name `%s` is longer than `%d` bytes", e.Name, e.Len)
}
