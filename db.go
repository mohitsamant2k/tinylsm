package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// DBOptions configures the database
type DBOptions struct {
	// Directory to store data files
	Dir string

	// MemtableSize is the max size before flushing (default 4MB)
	MemtableSize int64

	// SyncWrites ensures durability on every write (slower)
	SyncWrites bool
}

// DefaultOptions returns sensible defaults
func DefaultOptions(dir string) *DBOptions {
	return &DBOptions{
		Dir:          dir,
		MemtableSize: 4 * 1024 * 1024, // 4MB
		SyncWrites:   false,
	}
}

// DB is the main LSM-tree database
type DB struct {
	opts *DBOptions

	// Active memtable for writes
	memtable *Memtable

	// Immutable memtable being flushed (nil if not flushing)
	immutable *Memtable

	// Write-ahead log for active memtable
	wal *WAL

	// SSTables on disk (newest first)
	sstables []*SSTableReader

	// Next SSTable ID
	nextSSTableID uint64

	// Mutex for coordinating flushes
	mu sync.RWMutex

	// Is the DB closed?
	closed atomic.Bool
}

// Open opens or creates a database
func Open(opts *DBOptions) (*DB, error) {
	// Create directory if needed
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	db := &DB{
		opts:     opts,
		sstables: make([]*SSTableReader, 0),
	}

	// Clean up any temp files from crashed flushes
	db.cleanupTempFiles()

	// Load existing SSTables
	if err := db.loadSSTables(); err != nil {
		return nil, err
	}

	// Recover memtable from WAL (if exists)
	walPath := filepath.Join(opts.Dir, "wal.log")
	memtable, err := RecoverMemtable(walPath, opts.MemtableSize)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to recover from WAL: %w", err)
	}
	db.memtable = memtable

	// Open WAL for new writes (truncate old one since we recovered)
	wal, err := OpenWAL(walPath, opts.SyncWrites)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}
	db.wal = wal

	return db, nil
}

// cleanupTempFiles removes incomplete SSTable files
func (db *DB) cleanupTempFiles() {
	pattern := filepath.Join(db.opts.Dir, "*.tmp")
	files, _ := filepath.Glob(pattern)
	for _, f := range files {
		os.Remove(f)
	}
}

// loadSSTables loads all existing SSTables
func (db *DB) loadSSTables() error {
	pattern := filepath.Join(db.opts.Dir, "sst_*.sst")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return err
	}

	// Sort by ID (newest first for read order)
	sort.Slice(files, func(i, j int) bool {
		idI := db.parseSSTableID(files[i])
		idJ := db.parseSSTableID(files[j])
		return idI > idJ // Descending order (newest first)
	})

	for _, path := range files {
		reader, err := OpenSSTable(path, nil)
		if err != nil {
			// Log and skip corrupted SSTables
			fmt.Printf("Warning: skipping corrupted SSTable %s: %v\n", path, err)
			continue
		}
		db.sstables = append(db.sstables, reader)

		// Track highest ID
		id := db.parseSSTableID(path)
		if id >= db.nextSSTableID {
			db.nextSSTableID = id + 1
		}
	}

	return nil
}

// parseSSTableID extracts ID from filename like "sst_000001.sst"
func (db *DB) parseSSTableID(path string) uint64 {
	base := filepath.Base(path)
	base = strings.TrimPrefix(base, "sst_")
	base = strings.TrimSuffix(base, ".sst")
	id, _ := strconv.ParseUint(base, 10, 64)
	return id
}

// Put stores a key-value pair
func (db *DB) Put(key, value []byte) error {
	if db.closed.Load() {
		return ErrClosed
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Write to WAL first (for durability)
	if err := db.wal.WritePut(key, value); err != nil {
		return fmt.Errorf("WAL write failed: %w", err)
	}

	// Write to memtable
	if err := db.memtable.Put(key, value); err != nil {
		return err
	}

	// Check if memtable is full
	if db.memtable.IsFull() {
		if err := db.triggerFlush(); err != nil {
			return err
		}
	}

	return nil
}

// Delete removes a key (writes a tombstone)
func (db *DB) Delete(key []byte) error {
	if db.closed.Load() {
		return ErrClosed
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Write to WAL first
	if err := db.wal.WriteDelete(key); err != nil {
		return fmt.Errorf("WAL write failed: %w", err)
	}

	// Write tombstone to memtable
	if err := db.memtable.Delete(key); err != nil {
		return err
	}

	// Check if memtable is full
	if db.memtable.IsFull() {
		if err := db.triggerFlush(); err != nil {
			return err
		}
	}

	return nil
}

// Get retrieves a value by key
// Returns: (value, error)
// Returns ErrNotFound if key doesn't exist
// Returns nil value if key was deleted
func (db *DB) Get(key []byte) ([]byte, error) {
	if db.closed.Load() {
		return nil, ErrClosed
	}

	db.mu.RLock()
	defer db.mu.RUnlock()

	// 1. Check active memtable (newest data)
	if value, deleted, found := db.memtable.Get(key); found {
		if deleted {
			return nil, ErrNotFound // Key was deleted
		}
		return value, nil
	}

	// 2. Check immutable memtable (if flushing)
	if db.immutable != nil {
		if value, deleted, found := db.immutable.Get(key); found {
			if deleted {
				return nil, ErrNotFound
			}
			return value, nil
		}
	}

	// 3. Check SSTables (newest to oldest)
	for _, sst := range db.sstables {
		if value, deleted, found := sst.Get(key); found {
			if deleted {
				return nil, ErrNotFound
			}
			return value, nil
		}
	}

	return nil, ErrNotFound
}

// triggerFlush starts flushing the memtable to an SSTable
// Must be called with db.mu held
func (db *DB) triggerFlush() error {
	// If already flushing, wait (simple blocking approach)
	// In production, you'd use a background goroutine
	if db.immutable != nil {
		// Previous flush still in progress - do it synchronously
		if err := db.doFlush(); err != nil {
			return err
		}
	}

	// Make current memtable immutable
	db.memtable.SetImmutable()
	db.immutable = db.memtable

	// Create new active memtable
	db.memtable = NewMemtable(db.opts.MemtableSize)

	// Create new WAL (old WAL will be deleted after flush)
	oldWAL := db.wal
	walPath := filepath.Join(db.opts.Dir, "wal.log")

	// We need to close old WAL and create new one
	// For simplicity, we'll reuse the same path after flush completes

	// Perform the flush
	if err := db.doFlush(); err != nil {
		return err
	}

	// Now safe to remove WAL (data is in SSTable)
	// We delete instead of truncate - if delete fails but SSTable exists,
	// recovery will safely handle duplicates via idempotent overwrites
	oldWAL.Close()
	if err := os.Remove(walPath); err != nil && !os.IsNotExist(err) {
		// Log warning but continue - worst case is duplicate replay on restart
		// which is safe because memtable overwrites are idempotent
		fmt.Printf("Warning: failed to remove WAL: %v\n", err)
	}

	newWAL, err := OpenWAL(walPath, db.opts.SyncWrites)
	if err != nil {
		return err
	}
	db.wal = newWAL

	return nil
}

// doFlush writes the immutable memtable to an SSTable
func (db *DB) doFlush() error {
	if db.immutable == nil {
		return nil
	}

	// Generate SSTable path
	sstPath := filepath.Join(db.opts.Dir, fmt.Sprintf("sst_%06d.sst", db.nextSSTableID))
	db.nextSSTableID++

	// Flush memtable to SSTable (uses atomic rename internally)
	if err := FlushMemtableToSSTable(db.immutable, sstPath); err != nil {
		return fmt.Errorf("flush failed: %w", err)
	}

	// Open the new SSTable for reading
	reader, err := OpenSSTable(sstPath, nil)
	if err != nil {
		return fmt.Errorf("failed to open new SSTable: %w", err)
	}

	// Add to front of sstables list (newest first)
	db.sstables = append([]*SSTableReader{reader}, db.sstables...)

	// Clear immutable memtable
	db.immutable = nil

	return nil
}

// Close closes the database
func (db *DB) Close() error {
	if db.closed.Swap(true) {
		return nil // Already closed
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	var firstErr error

	// Flush any remaining data
	if db.immutable != nil {
		if err := db.doFlush(); err != nil {
			firstErr = err
		}
	}

	// Close WAL
	if db.wal != nil {
		if err := db.wal.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Close all SSTables
	for _, sst := range db.sstables {
		if err := sst.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	return firstErr
}

// Stats returns database statistics
type Stats struct {
	MemtableSize   int64
	ImmutableSize  int64
	SSTableCount   int
	TotalDiskUsage int64
}

func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()

	stats := Stats{
		MemtableSize: db.memtable.Size(),
		SSTableCount: len(db.sstables),
	}

	if db.immutable != nil {
		stats.ImmutableSize = db.immutable.Size()
	}

	// Calculate disk usage
	for _, sst := range db.sstables {
		if info, err := os.Stat(sst.Path()); err == nil {
			stats.TotalDiskUsage += info.Size()
		}
	}

	return stats
}
