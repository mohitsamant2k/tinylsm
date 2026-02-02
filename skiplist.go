package lsm

import (
	"sync"
)

const (
	maxLevel    = 12
	probability = 4
)

type skipNode struct {
	entry   *Entry
	forward []*skipNode
}

// SkipList is a concurrent-safe sorted in-memory structure
type SkipList struct {
	head       *skipNode
	comparator Comparator
	level      int
	size       int64
	count      int
	randSeed   uint32
	mu         sync.RWMutex // <- ADD THIS for thread safety
}

// NewSkipList creates a skip list with default comparator
func NewSkipList() *SkipList {
	return NewSkipListWithComparator(DefaultComparator{})
}

func NewSkipListWithComparator(cmp Comparator) *SkipList {
	return &SkipList{
		head: &skipNode{
			forward: make([]*skipNode, maxLevel),
		},
		comparator: cmp,
		level:      1,
		randSeed:   0xdeadbeef,
	}
}

func (sl *SkipList) randomLevel() int {
	level := 1
	sl.randSeed = sl.randSeed*1664525 + 1013904223
	r := sl.randSeed
	for level < maxLevel && r%probability == 0 {
		level++
		r /= probability
	}
	return level
}

func (sl *SkipList) compare(a, b []byte) int {
	return sl.comparator.Compare(a, b)
}

// Put inserts or updates an entry (thread-safe)
func (sl *SkipList) Put(key, value []byte) {
	sl.PutEntry(NewEntry(key, value))
}

// Delete marks a key as deleted (thread-safe)
func (sl *SkipList) Delete(key []byte) {
	sl.PutEntry(NewTombstone(key))
}

// PutEntry inserts an entry (thread-safe)
func (sl *SkipList) PutEntry(entry *Entry) {
	sl.mu.Lock() // <- WRITE LOCK
	defer sl.mu.Unlock()

	update := make([]*skipNode, maxLevel)
	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil &&
			sl.compare(current.forward[i].entry.Key, entry.Key) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	current = current.forward[0]

	// key already exists, update value
	if current != nil && sl.compare(current.entry.Key, entry.Key) == 0 {
		oldSize := current.entry.Size()
		current.entry.Value = entry.Value
		current.entry.Deleted = entry.Deleted
		current.entry.Timestamp = entry.Timestamp
		sl.size += entry.Size() - oldSize
		return
	}

	// adding the new level to the linked list
	newLevel := sl.randomLevel()
	if newLevel > sl.level {
		for i := sl.level; i < newLevel; i++ {
			update[i] = sl.head
		}
		sl.level = newLevel
	}

	newNode := &skipNode{
		entry:   entry,
		forward: make([]*skipNode, newLevel),
	}

	for i := 0; i < newLevel; i++ {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	sl.size += entry.Size()
	sl.count++
}

// Get retrieves a value by key (thread-safe)
func (sl *SkipList) Get(key []byte) ([]byte, bool, bool) {
	sl.mu.RLock() // <- READ LOCK (multiple readers allowed)
	defer sl.mu.RUnlock()

	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil &&
			sl.compare(current.forward[i].entry.Key, key) < 0 {
			current = current.forward[i]
		}
	}

	current = current.forward[0]

	if current != nil && sl.compare(current.entry.Key, key) == 0 {
		return current.entry.Value, current.entry.Deleted, true // (value, deleted, found)
	}

	return nil, false, false // (value, deleted, found)
}

// Size returns approximate memory usage (thread-safe)
func (sl *SkipList) Size() int64 {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.size
}

// Count returns number of entries (thread-safe)
func (sl *SkipList) Count() int {
	sl.mu.RLock()
	defer sl.mu.RUnlock()
	return sl.count
}

// Iterator for traversing the skip list
// NOTE: Iterator holds a read lock - don't forget to close it!
type SkipListIterator struct {
	list    *SkipList
	current *skipNode
}

// NewIterator creates an iterator (acquires read lock)
func (sl *SkipList) NewIterator() *SkipListIterator {
	sl.mu.RLock() // <- Lock acquired here
	return &SkipListIterator{
		list:    sl,
		current: sl.head,
	}
}

// Close releases the read lock - MUST be called!
func (it *SkipListIterator) Close() {
	it.list.mu.RUnlock() // <- Release lock
}

// SeekToFirst moves to the first entry
func (it *SkipListIterator) SeekToFirst() {
	it.current = it.list.head.forward[0]
}

// Seek moves to first entry >= target
func (it *SkipListIterator) Seek(target []byte) {
	current := it.list.head
	for i := it.list.level - 1; i >= 0; i-- {
		for current.forward[i] != nil &&
			it.list.compare(current.forward[i].entry.Key, target) < 0 {
			current = current.forward[i]
		}
	}
	it.current = current.forward[0]
}

// Valid returns true if at a valid entry
func (it *SkipListIterator) Valid() bool {
	return it.current != nil
}

// Next moves to next entry
func (it *SkipListIterator) Next() {
	if it.current != nil {
		it.current = it.current.forward[0]
	}
}

// Entry returns current entry
func (it *SkipListIterator) Entry() *Entry {
	if it.current != nil {
		return it.current.entry
	}
	return nil
}

// Key returns current key
func (it *SkipListIterator) Key() []byte {
	if it.current != nil {
		return it.current.entry.Key
	}
	return nil
}

// Value returns current value
func (it *SkipListIterator) Value() []byte {
	if it.current != nil {
		return it.current.entry.Value
	}
	return nil
}

// IsDeleted returns true if current entry is a tombstone
func (it *SkipListIterator) IsDeleted() bool {
	if it.current != nil {
		return it.current.entry.Deleted
	}
	return false
}
