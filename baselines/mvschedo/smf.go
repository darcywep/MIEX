package mvschedo

import (
	"math"
	"math/rand"
)

const defaultSMFSampleSize = 5

type SMFScheduler struct {
	sampleSize int
	rng        *rand.Rand
}

type keyMakespanState struct {
	readReady  int
	writeReady int
}

func NewSMFScheduler(sampleSize int, seed int64) *SMFScheduler {
	if sampleSize <= 0 {
		sampleSize = defaultSMFSampleSize
	}
	return &SMFScheduler{
		sampleSize: sampleSize,
		rng:        rand.New(rand.NewSource(seed)),
	}
}

func (s *SMFScheduler) Schedule(txs []*MVSchedOTransaction) []*MVSchedOTransaction {
	if len(txs) <= 1 {
		scheduled := append([]*MVSchedOTransaction(nil), txs...)
		for idx, tx := range scheduled {
			tx.Timestamp = uint64(idx + 1)
		}
		return scheduled
	}

	unscheduled := append([]*MVSchedOTransaction(nil), txs...)
	scheduled := make([]*MVSchedOTransaction, 0, len(txs))
	states := make(map[string]keyMakespanState)
	makespan := 0

	firstIndex := s.rng.Intn(len(unscheduled))
	first := unscheduled[firstIndex]
	unscheduled = removeAt(unscheduled, firstIndex)
	states, makespan = simulateAppend(first, states, makespan)
	scheduled = append(scheduled, first)

	for len(unscheduled) > 0 {
		sampleIndexes := s.sampleIndexes(len(unscheduled))
		bestSamplePos := -1
		bestDelta := math.MaxInt
		bestMakespan := 0
		var bestStates map[string]keyMakespanState

		for _, idx := range sampleIndexes {
			candidate := unscheduled[idx]
			candidateStates, candidateMakespan := simulateAppend(candidate, states, makespan)
			delta := candidateMakespan - makespan
			if bestSamplePos < 0 ||
				delta < bestDelta ||
				(delta == bestDelta && candidate.ArrivalID < unscheduled[bestSamplePos].ArrivalID) {
				bestSamplePos = idx
				bestDelta = delta
				bestMakespan = candidateMakespan
				bestStates = candidateStates
			}
		}

		chosen := unscheduled[bestSamplePos]
		unscheduled = removeAt(unscheduled, bestSamplePos)
		states = bestStates
		makespan = bestMakespan
		scheduled = append(scheduled, chosen)
	}

	for idx, tx := range scheduled {
		tx.Timestamp = uint64(idx + 1)
	}
	return scheduled
}

func (s *SMFScheduler) sampleIndexes(n int) []int {
	if n <= s.sampleSize {
		indexes := make([]int, n)
		for i := 0; i < n; i++ {
			indexes[i] = i
		}
		return indexes
	}

	seen := make(map[int]struct{}, s.sampleSize)
	indexes := make([]int, 0, s.sampleSize)
	for len(indexes) < s.sampleSize {
		idx := s.rng.Intn(n)
		if _, ok := seen[idx]; ok {
			continue
		}
		seen[idx] = struct{}{}
		indexes = append(indexes, idx)
	}
	return indexes
}

func simulateAppend(tx *MVSchedOTransaction, states map[string]keyMakespanState, currentMakespan int) (map[string]keyMakespanState, int) {
	nextStates := cloneMakespanStates(states)
	txTime := 0
	makespan := currentMakespan

	for _, op := range tx.Ops {
		state := nextStates[op.Key]
		var start int
		if op.Type == ReadOperation {
			start = maxInt(txTime, state.writeReady)
			finish := start + 1
			state.readReady = maxInt(state.readReady, finish)
			nextStates[op.Key] = state
			txTime = finish
			makespan = maxInt(makespan, finish)
			continue
		}

		start = maxInt(txTime, maxInt(state.readReady, state.writeReady))
		finish := start + 1
		state.writeReady = finish
		nextStates[op.Key] = state
		txTime = finish
		makespan = maxInt(makespan, finish)
	}

	return nextStates, makespan
}

func cloneMakespanStates(states map[string]keyMakespanState) map[string]keyMakespanState {
	cloned := make(map[string]keyMakespanState, len(states))
	for key, value := range states {
		cloned[key] = value
	}
	return cloned
}

func removeAt(txs []*MVSchedOTransaction, idx int) []*MVSchedOTransaction {
	copy(txs[idx:], txs[idx+1:])
	return txs[:len(txs)-1]
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
