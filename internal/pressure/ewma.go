package pressure

import (
	"math"
	"time"
)

type EWMA struct {
	window      time.Duration
	initialized bool
	lastAt      time.Time
	value       float64
}

func NewEWMA(window time.Duration) *EWMA {
	return &EWMA{window: window}
}

func (e *EWMA) Add(at time.Time, value float64) float64 {
	if !e.initialized {
		e.initialized = true
		e.lastAt = at
		e.value = value
		return e.value
	}
	if !at.After(e.lastAt) {
		return e.value
	}
	if e.window <= 0 {
		e.lastAt = at
		e.value = value
		return e.value
	}
	elapsed := at.Sub(e.lastAt)
	alpha := 1 - math.Exp(-float64(elapsed)/float64(e.window))
	e.value += alpha * (value - e.value)
	e.lastAt = at
	return e.value
}

func (e *EWMA) Value() (float64, bool) {
	return e.value, e.initialized
}
