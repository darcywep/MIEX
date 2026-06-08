package mvschedo

import "sync"

type mvccVersion struct {
	timestamp uint64
	value     string
	writer    *MVSchedOTransaction
	committed bool
	aborted   bool
}

type mvccKeyEntry struct {
	mu        sync.Mutex
	maxReadTS uint64
	versions  []mvccVersion
}

type MVCCTable struct {
	mu      sync.Mutex
	entries map[string]*mvccKeyEntry
}

func NewMVCCTable() *MVCCTable {
	return &MVCCTable{
		entries: make(map[string]*mvccKeyEntry),
	}
}

func (t *MVCCTable) Read(tx *MVSchedOTransaction, key string) string {
	entry := t.entryForKey(key)

	entry.mu.Lock()
	defer entry.mu.Unlock()

	entry.ensureInitialVersion()
	bestIdx := -1
	for idx := range entry.versions {
		version := entry.versions[idx]
		if version.aborted || version.timestamp > tx.Timestamp {
			continue
		}
		if bestIdx < 0 || version.timestamp > entry.versions[bestIdx].timestamp {
			bestIdx = idx
		}
	}

	if tx.Timestamp > entry.maxReadTS {
		entry.maxReadTS = tx.Timestamp
	}

	if bestIdx < 0 {
		return ""
	}

	version := entry.versions[bestIdx]
	if version.writer != nil && !version.committed {
		tx.AddDependency(version.writer)
	}
	return version.value
}

func (t *MVCCTable) Write(tx *MVSchedOTransaction, key string, value string) bool {
	entry := t.entryForKey(key)

	entry.mu.Lock()
	defer entry.mu.Unlock()

	entry.ensureInitialVersion()
	if tx.Timestamp < entry.maxReadTS {
		return false
	}

	for idx := range entry.versions {
		version := &entry.versions[idx]
		if version.writer == tx && version.timestamp == tx.Timestamp {
			version.value = value
			version.aborted = false
			return true
		}
	}

	entry.versions = append(entry.versions, mvccVersion{
		timestamp: tx.Timestamp,
		value:     value,
		writer:    tx,
		committed: false,
		aborted:   false,
	})
	return true
}

func (t *MVCCTable) WaitDependencies(tx *MVSchedOTransaction) bool {
	for _, dep := range tx.Dependencies() {
		if !dep.WaitFinalStatus() {
			return false
		}
	}
	return true
}

func (t *MVCCTable) MarkCommitted(tx *MVSchedOTransaction) {
	for key := range tx.LocalPut {
		entry := t.entryForKey(key)
		entry.mu.Lock()
		for idx := range entry.versions {
			version := &entry.versions[idx]
			if version.writer == tx && version.timestamp == tx.Timestamp {
				version.committed = true
				version.aborted = false
			}
		}
		entry.mu.Unlock()
	}
	tx.MarkCommitted()
}

func (t *MVCCTable) Abort(tx *MVSchedOTransaction) {
	for key := range tx.LocalPut {
		entry := t.entryForKey(key)
		entry.mu.Lock()
		for idx := range entry.versions {
			version := &entry.versions[idx]
			if version.writer == tx && version.timestamp == tx.Timestamp {
				version.aborted = true
				version.committed = false
			}
		}
		entry.mu.Unlock()
	}
	tx.MarkAborted()
}

func (t *MVCCTable) entryForKey(key string) *mvccKeyEntry {
	t.mu.Lock()
	defer t.mu.Unlock()

	entry := t.entries[key]
	if entry == nil {
		entry = &mvccKeyEntry{}
		t.entries[key] = entry
	}
	return entry
}

func (e *mvccKeyEntry) ensureInitialVersion() {
	if len(e.versions) > 0 {
		return
	}
	e.versions = append(e.versions, mvccVersion{
		timestamp: 0,
		value:     "",
		committed: true,
	})
}
