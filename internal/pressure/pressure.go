package pressure

import (
	"errors"
	"math"
)

const (
	queueWeight   = .55
	kvWeight      = .30
	runningWeight = .15
)

type Sample struct {
	Running float64
	Waiting float64
	KVUsage float64
}

type Limits struct {
	QueueSoft   float64
	KVSoft      float64
	KVHard      float64
	RunningSoft float64
}

func (l Limits) Validate() error {
	if !positiveFinite(l.QueueSoft) {
		return errors.New("queue soft limit must be a positive finite number")
	}
	if !positiveFinite(l.RunningSoft) {
		return errors.New("running soft limit must be a positive finite number")
	}
	if !finite(l.KVSoft) || !finite(l.KVHard) || l.KVSoft < 0 || l.KVHard > 1 || l.KVSoft >= l.KVHard {
		return errors.New("KV limits must satisfy 0 <= soft < hard <= 1")
	}
	return nil
}

func Calculate(sample Sample, limits Limits) float64 {
	queue := clamp(sample.Waiting/limits.QueueSoft, 0, 2)
	kv := clamp((sample.KVUsage-limits.KVSoft)/(limits.KVHard-limits.KVSoft), 0, 2)
	running := clamp(sample.Running/limits.RunningSoft, 0, 2)
	return queueWeight*queue + kvWeight*kv + runningWeight*running
}

func clamp(value, minimum, maximum float64) float64 {
	if math.IsNaN(value) {
		return minimum
	}
	if value < minimum {
		return minimum
	}
	if value > maximum {
		return maximum
	}
	return value
}

func positiveFinite(value float64) bool {
	return value > 0 && finite(value)
}

func finite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
