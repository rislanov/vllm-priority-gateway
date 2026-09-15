package local

import (
	"container/heap"
	"time"

	"github.com/google/uuid"
)

type expiryKind uint8

const (
	admissionLeaseExpiry expiryKind = iota
	admissionReceiptExpiry
	circuitPermitExpiry
	circuitAttemptExpiry
)

type expiryEntry struct {
	at   time.Time
	id   uuid.UUID
	kind expiryKind
}

type expiryQueue []expiryEntry

func (q expiryQueue) Len() int           { return len(q) }
func (q expiryQueue) Less(i, j int) bool { return q[i].at.Before(q[j].at) }
func (q expiryQueue) Swap(i, j int)      { q[i], q[j] = q[j], q[i] }
func (q *expiryQueue) Push(value any)    { *q = append(*q, value.(expiryEntry)) }
func (q *expiryQueue) Pop() any {
	old := *q
	last := len(old) - 1
	value := old[last]
	old[last] = expiryEntry{}
	*q = old[:last]
	return value
}

func (q *expiryQueue) add(kind expiryKind, id uuid.UUID, at time.Time) {
	heap.Push(q, expiryEntry{kind: kind, id: id, at: at})
}

func (q *expiryQueue) due(now time.Time) (expiryEntry, bool) {
	if q.Len() == 0 || (*q)[0].at.After(now) {
		return expiryEntry{}, false
	}
	return heap.Pop(q).(expiryEntry), true
}

func (q *expiryQueue) popOldest() (expiryEntry, bool) {
	if q.Len() == 0 {
		return expiryEntry{}, false
	}
	return heap.Pop(q).(expiryEntry), true
}
