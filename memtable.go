package lsm

import (
	"sync"
	"sync/atomic"
)

const (
	memtableActive    = 0
	memtableImmutable = 1
)

// Entry represents a key-value pair in the memtable.
type Entry struct {
	Key       []byte
	Value     []byte
	Deleted   bool   // Tombstone flag
	Timestamp uint64 // for MVCC: version/timestamp (0 for now)
}

func (e *Entry) Size() int64 {
	return int64(len(e.Key) + len(e.Value) + 1 + 8) // key + value + deleted flag + timestamp
}

// NewEntry creates a new entry with the given key and value.
func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:       key,
		Value:     value,
		Deleted:   false,
		Timestamp: 0, // Will be used for MVCC later
	}
}

// NewTombstone creates a deletion marker
func NewTombstone(key []byte) *Entry {
	return &Entry{
		Key:       key,
		Value:     nil,
		Deleted:   true,
		Timestamp: 0,
	}
}

type Memtable struct {
	data    *SkipList // underlying skip list
	state   int32
	maxsize int64      // maximum size before flush
	mu      sync.Mutex // protects state transitions
}

// NewMemtable initializes and returns a new Memtable.
func NewMemtable(maxsize int64) *Memtable {
	return &Memtable{
		data:    NewSkipList(),
		state:   memtableActive,
		maxsize: maxsize,
	}
}

// Put inserts a key-value pair (thread-safe)
func (m *Memtable) Put(key, value []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if atomic.LoadInt32(&m.state) != memtableActive {
		return ErrMemtableImmutable
	}
	m.data.Put(key, value)
	return nil
}

// Delete marks a key as deleted (thread-safe)
func (m *Memtable) Delete(key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if atomic.LoadInt32(&m.state) != memtableActive {
		return ErrMemtableImmutable
	}
	m.data.Delete(key)
	return nil
}

// Get retrieves a value by key
// Returns: (value, found, deleted)
func (m *Memtable) Get(key []byte) ([]byte, bool, bool) {
	return m.data.Get(key)
}

// Size returns current size in bytes
func (m *Memtable) Size() int64 {
	return m.data.Size()
}

// IsFull returns true if memtable should be flushed
func (m *Memtable) IsFull() bool {
	return m.data.Size() >= m.maxsize
}

// SetImmutable freezes the memtable (thread-safe)
func (m *Memtable) SetImmutable() {
	m.mu.Lock()
	defer m.mu.Unlock()
	atomic.StoreInt32(&m.state, memtableImmutable)
}

// IsImmutable returns true if frozen
func (m *Memtable) IsImmutable() bool {
	return atomic.LoadInt32(&m.state) == memtableImmutable
}

// NewIterator creates an iterator
func (m *Memtable) NewIterator() *SkipListIterator {
	return m.data.NewIterator()
}

// Count returns number of entries
func (m *Memtable) Count() int {
	return m.data.Count()
}
