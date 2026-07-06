package cu

import (
	"errors"
)

var ErrComputeExceeded = errors.New("Compute exceeded")

type ComputeMeter struct {
	computeMeter    uint64
	startingBalance uint64
	disable         bool
}

func NewComputeMeter(budget uint64) ComputeMeter {
	return ComputeMeter{computeMeter: budget, startingBalance: budget}
}

func NewComputeMeterDefault() ComputeMeter {
	return ComputeMeter{computeMeter: 200000, startingBalance: 200000}
}

func (cm *ComputeMeter) Consume(cost uint64) error {
	if cm.disable {
		return nil
	}

	if cm.computeMeter < cost {
		cm.computeMeter = 0
		return ErrComputeExceeded
	}
	cm.computeMeter -= cost
	return nil
}

func (cm *ComputeMeter) Used() uint64 {
	return cm.startingBalance - cm.computeMeter
}

func (cm *ComputeMeter) Remaining() uint64 {
	return cm.computeMeter
}

func (cm *ComputeMeter) Disabled() bool {
	return cm.disable
}

func (cm *ComputeMeter) Disable() {
	cm.disable = true
}

func (cm *ComputeMeter) Enable() {
	cm.disable = false
}
