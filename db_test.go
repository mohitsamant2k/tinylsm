package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestDBBasicOperations(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Put
	if err := db.Put([]byte("hello"), []byte("world")); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get
	value, err := db.Get([]byte("hello"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(value) != "world" {
		t.Errorf("Expected 'world', got '%s'", string(value))
	}

	// Get non-existent
	_, err = db.Get([]byte("notfound"))
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}

func TestDBDelete(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Put then delete
	db.Put([]byte("key1"), []byte("value1"))

	// Verify exists
	_, err = db.Get([]byte("key1"))
	if err != nil {
		t.Fatalf("Key should exist before delete")
	}

	// Delete
	if err := db.Delete([]byte("key1")); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify deleted
	_, err = db.Get([]byte("key1"))
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound after delete, got %v", err)
	}
}

func TestDBPersistence(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)

	// Write some data
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	db.Put([]byte("persist1"), []byte("value1"))
	db.Put([]byte("persist2"), []byte("value2"))
	db.Close()

	// Reopen and verify
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen DB: %v", err)
	}
	defer db2.Close()

	value, err := db2.Get([]byte("persist1"))
	if err != nil {
		t.Fatalf("Key persist1 not found after reopen: %v", err)
	}
	if string(value) != "value1" {
		t.Errorf("Expected 'value1', got '%s'", string(value))
	}

	value, err = db2.Get([]byte("persist2"))
	if err != nil {
		t.Fatalf("Key persist2 not found after reopen: %v", err)
	}
	if string(value) != "value2" {
		t.Errorf("Expected 'value2', got '%s'", string(value))
	}
}

func TestDBFlush(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 1024 // Small memtable to trigger flush

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Write enough data to trigger flush
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%05d", i)
		value := fmt.Sprintf("value_%05d_padding_to_make_it_bigger", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			t.Fatalf("Put failed at %d: %v", i, err)
		}
	}

	// Check that SSTables were created
	stats := db.Stats()
	if stats.SSTableCount == 0 {
		t.Error("Expected at least one SSTable after many writes")
	}
	t.Logf("Created %d SSTables", stats.SSTableCount)

	// Verify all data is readable
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key_%05d", i)
		expectedValue := fmt.Sprintf("value_%05d_padding_to_make_it_bigger", i)
		value, err := db.Get([]byte(key))
		if err != nil {
			t.Errorf("Key %s not found: %v", key, err)
			continue
		}
		if string(value) != expectedValue {
			t.Errorf("Key %s: expected %s, got %s", key, expectedValue, string(value))
		}
	}
}

func TestDBRecoveryAfterFlush(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512 // Very small to trigger multiple flushes

	// Write data causing multiple flushes
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("recovery_key_%03d", i)
		value := fmt.Sprintf("recovery_value_%03d_with_extra_data", i)
		db.Put([]byte(key), []byte(value))
	}

	sstCount := db.Stats().SSTableCount
	t.Logf("Created %d SSTables before close", sstCount)
	db.Close()

	// Reopen and verify
	db2, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer db2.Close()

	// Verify SSTable count
	if db2.Stats().SSTableCount != sstCount {
		t.Errorf("SSTable count changed: expected %d, got %d",
			sstCount, db2.Stats().SSTableCount)
	}

	// Verify all data
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("recovery_key_%03d", i)
		expectedValue := fmt.Sprintf("recovery_value_%03d_with_extra_data", i)
		value, err := db2.Get([]byte(key))
		if err != nil {
			t.Errorf("Key %s not found after recovery: %v", key, err)
			continue
		}
		if string(value) != expectedValue {
			t.Errorf("Key %s: wrong value after recovery", key)
		}
	}
}

func TestDBOverwrite(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Write initial value
	db.Put([]byte("key"), []byte("value1"))

	// Overwrite
	db.Put([]byte("key"), []byte("value2"))

	// Should get latest value
	value, err := db.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(value) != "value2" {
		t.Errorf("Expected 'value2', got '%s'", string(value))
	}
}

func TestDBOverwriteAcrossFlush(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 256 // Tiny to force flush

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Write initial value
	db.Put([]byte("overwrite_key"), []byte("old_value"))

	// Write enough to trigger flush
	for i := 0; i < 20; i++ {
		db.Put([]byte(fmt.Sprintf("filler_%d", i)), []byte("data_to_fill_memtable"))
	}

	// Now overwrite (in new memtable, old value in SSTable)
	db.Put([]byte("overwrite_key"), []byte("new_value"))

	// Should get new value (memtable should be checked before SSTable)
	value, err := db.Get([]byte("overwrite_key"))
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if string(value) != "new_value" {
		t.Errorf("Expected 'new_value', got '%s'", string(value))
	}
}

func TestDBDeleteAcrossFlush(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 256

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Write initial value
	db.Put([]byte("delete_key"), []byte("some_value"))

	// Trigger flush
	for i := 0; i < 20; i++ {
		db.Put([]byte(fmt.Sprintf("filler_%d", i)), []byte("data_to_fill_memtable"))
	}

	// Delete (tombstone in memtable, value in SSTable)
	db.Delete([]byte("delete_key"))

	// Should not find (tombstone should shadow SSTable value)
	_, err = db.Get([]byte("delete_key"))
	if err != ErrNotFound {
		t.Errorf("Expected ErrNotFound, got %v", err)
	}
}

func TestDBConcurrentReads(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Write some data
	for i := 0; i < 100; i++ {
		db.Put([]byte(fmt.Sprintf("key_%d", i)), []byte(fmt.Sprintf("value_%d", i)))
	}

	// Concurrent reads
	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 100; i++ {
				key := fmt.Sprintf("key_%d", i%100)
				_, err := db.Get([]byte(key))
				if err != nil && err != ErrNotFound {
					t.Errorf("Concurrent read failed: %v", err)
				}
			}
		}()
	}
	wg.Wait()
}

func TestDBConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 4096 // Reasonable size

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Concurrent writes
	var wg sync.WaitGroup
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(goroutine int) {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				key := fmt.Sprintf("g%d_key_%d", goroutine, i)
				value := fmt.Sprintf("g%d_value_%d", goroutine, i)
				if err := db.Put([]byte(key), []byte(value)); err != nil {
					t.Errorf("Concurrent write failed: %v", err)
				}
			}
		}(g)
	}
	wg.Wait()

	// Verify all writes
	for g := 0; g < 5; g++ {
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("g%d_key_%d", g, i)
			expectedValue := fmt.Sprintf("g%d_value_%d", g, i)
			value, err := db.Get([]byte(key))
			if err != nil {
				t.Errorf("Key %s not found: %v", key, err)
				continue
			}
			if string(value) != expectedValue {
				t.Errorf("Key %s: wrong value", key)
			}
		}
	}
}

func TestDBCleanupTempFiles(t *testing.T) {
	dir := t.TempDir()

	// Create some temp files (simulating crashed flush)
	os.WriteFile(filepath.Join(dir, "sst_000001.sst.tmp"), []byte("incomplete"), 0644)
	os.WriteFile(filepath.Join(dir, "sst_000002.sst.tmp"), []byte("incomplete"), 0644)

	opts := DefaultOptions(dir)
	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	db.Close()

	// Temp files should be cleaned up
	files, _ := filepath.Glob(filepath.Join(dir, "*.tmp"))
	if len(files) > 0 {
		t.Errorf("Temp files not cleaned up: %v", files)
	}
}

func TestDBStats(t *testing.T) {
	dir := t.TempDir()
	opts := DefaultOptions(dir)
	opts.MemtableSize = 512

	db, err := Open(opts)
	if err != nil {
		t.Fatalf("Failed to open DB: %v", err)
	}
	defer db.Close()

	// Initial stats
	stats := db.Stats()
	if stats.SSTableCount != 0 {
		t.Errorf("Expected 0 SSTables initially, got %d", stats.SSTableCount)
	}

	// Write data
	for i := 0; i < 50; i++ {
		db.Put([]byte(fmt.Sprintf("key_%d", i)), []byte(fmt.Sprintf("value_%d_with_padding", i)))
	}

	stats = db.Stats()
	t.Logf("Stats: MemtableSize=%d, SSTableCount=%d, DiskUsage=%d",
		stats.MemtableSize, stats.SSTableCount, stats.TotalDiskUsage)

	if stats.SSTableCount == 0 {
		t.Error("Expected some SSTables after writes")
	}
	if stats.TotalDiskUsage == 0 {
		t.Error("Expected non-zero disk usage")
	}
}
