package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSSTableWriteRead(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	// Write SSTable
	writer, err := NewSSTableWriter(path, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Add entries (must be in sorted order!)
	entries := []struct {
		key     string
		value   string
		deleted bool
	}{
		{"apple", "red", false},
		{"banana", "yellow", false},
		{"cherry", "red", false},
		{"grape", "purple", false},
		{"mango", "orange", false},
	}

	for _, e := range entries {
		if err := writer.Add([]byte(e.key), []byte(e.value), e.deleted); err != nil {
			t.Fatalf("Failed to add entry: %v", err)
		}
	}

	if err := writer.Finish(); err != nil {
		t.Fatalf("Failed to finish: %v", err)
	}

	// Read SSTable
	reader, err := OpenSSTable(path, nil)
	if err != nil {
		t.Fatalf("Failed to open SSTable: %v", err)
	}
	defer reader.Close()

	// Test lookups
	for _, e := range entries {
		value, deleted, found := reader.Get([]byte(e.key))
		if !found {
			t.Errorf("Key %s not found", e.key)
			continue
		}
		if deleted != e.deleted {
			t.Errorf("Key %s: expected deleted=%v, got %v", e.key, e.deleted, deleted)
		}
		if string(value) != e.value {
			t.Errorf("Key %s: expected value %s, got %s", e.key, e.value, string(value))
		}
	}

	// Test non-existent key
	_, _, found := reader.Get([]byte("notfound"))
	if found {
		t.Error("Expected notfound key to not be found")
	}
}

func TestSSTableWithDeletes(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	writer, err := NewSSTableWriter(path, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Add some entries with deletes (tombstones)
	writer.Add([]byte("a"), []byte("1"), false)
	writer.Add([]byte("b"), []byte(""), true) // deleted!
	writer.Add([]byte("c"), []byte("3"), false)

	if err := writer.Finish(); err != nil {
		t.Fatalf("Failed to finish: %v", err)
	}

	reader, err := OpenSSTable(path, nil)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer reader.Close()

	// Check regular entry
	value, deleted, found := reader.Get([]byte("a"))
	if !found || deleted || string(value) != "1" {
		t.Errorf("Entry 'a' incorrect: found=%v, deleted=%v, value=%s", found, deleted, value)
	}

	// Check deleted entry (tombstone)
	value, deleted, found = reader.Get([]byte("b"))
	if !found {
		t.Error("Tombstone 'b' should be found")
	}
	if !deleted {
		t.Error("Entry 'b' should be marked as deleted")
	}
}

func TestSSTableIterator(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	writer, err := NewSSTableWriter(path, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	keys := []string{"a", "b", "c", "d", "e"}
	for _, k := range keys {
		writer.Add([]byte(k), []byte(k+"_value"), false)
	}
	writer.Finish()

	reader, err := OpenSSTable(path, nil)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer reader.Close()

	// Iterate and collect all keys
	iter := reader.NewIterator()
	var collected []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		collected = append(collected, string(iter.Key()))
	}

	if len(collected) != len(keys) {
		t.Errorf("Expected %d keys, got %d", len(keys), len(collected))
	}

	for i, k := range keys {
		if i >= len(collected) {
			break
		}
		if collected[i] != k {
			t.Errorf("At index %d: expected %s, got %s", i, k, collected[i])
		}
	}
}

func TestSSTableMultipleBlocks(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	writer, err := NewSSTableWriter(path, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	// Write enough data to create multiple blocks
	// Each entry is roughly 100 bytes, BlockSize is 4KB
	// So ~40+ entries should create multiple blocks
	numEntries := 100
	for i := 0; i < numEntries; i++ {
		key := []byte(fmt.Sprintf("key_%05d", i)) // Sorted order
		value := make([]byte, 100)                // 100 byte value
		for j := range value {
			value[j] = byte(i % 256)
		}
		if err := writer.Add(key, value, false); err != nil {
			t.Fatalf("Failed to add entry %d: %v", i, err)
		}
	}

	if err := writer.Finish(); err != nil {
		t.Fatalf("Failed to finish: %v", err)
	}

	// Verify we created multiple blocks
	reader, err := OpenSSTable(path, nil)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer reader.Close()

	if len(reader.index) <= 1 {
		t.Errorf("Expected multiple blocks, got %d", len(reader.index))
	}

	t.Logf("Created %d blocks", len(reader.index))

	// Test lookups across different blocks
	testKeys := []int{0, 25, 50, 75, 99}
	for _, i := range testKeys {
		key := []byte(fmt.Sprintf("key_%05d", i))
		value, deleted, found := reader.Get(key)
		if !found {
			t.Errorf("Key %d not found", i)
			continue
		}
		if deleted {
			t.Errorf("Key %d should not be deleted", i)
		}
		if len(value) != 100 {
			t.Errorf("Key %d: expected 100 byte value, got %d", i, len(value))
		}
	}

	// Test key not in table
	_, _, found := reader.Get([]byte("key_99999"))
	if found {
		t.Error("Should not find key_99999")
	}
}

func TestFlushMemtableToSSTable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	// Create and populate memtable
	mem := NewMemtable(1024 * 1024)
	mem.Put([]byte("zebra"), []byte("striped"))
	mem.Put([]byte("apple"), []byte("red"))
	mem.Put([]byte("banana"), []byte("yellow"))
	mem.Delete([]byte("cherry")) // Add a tombstone

	// Flush to SSTable
	if err := FlushMemtableToSSTable(mem, path); err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// Verify SSTable contents
	reader, err := OpenSSTable(path, nil)
	if err != nil {
		t.Fatalf("Failed to open: %v", err)
	}
	defer reader.Close()

	// Check regular entries
	tests := []struct {
		key         string
		value       string
		deleted     bool
		shouldExist bool
	}{
		{"apple", "red", false, true},
		{"banana", "yellow", false, true},
		{"cherry", "", true, true}, // Tombstone
		{"zebra", "striped", false, true},
		{"notfound", "", false, false},
	}

	for _, tc := range tests {
		value, deleted, found := reader.Get([]byte(tc.key))
		if found != tc.shouldExist {
			t.Errorf("Key %s: expected found=%v, got %v", tc.key, tc.shouldExist, found)
			continue
		}
		if !found {
			continue
		}
		if deleted != tc.deleted {
			t.Errorf("Key %s: expected deleted=%v, got %v", tc.key, tc.deleted, deleted)
		}
		if !tc.deleted && string(value) != tc.value {
			t.Errorf("Key %s: expected value=%s, got %s", tc.key, tc.value, string(value))
		}
	}

	// Verify iteration order (should be sorted!)
	iter := reader.NewIterator()
	var keys []string
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		keys = append(keys, string(iter.Key()))
	}

	expectedOrder := []string{"apple", "banana", "cherry", "zebra"}
	if len(keys) != len(expectedOrder) {
		t.Fatalf("Expected %d keys, got %d", len(expectedOrder), len(keys))
	}
	for i, k := range expectedOrder {
		if keys[i] != k {
			t.Errorf("At index %d: expected %s, got %s", i, k, keys[i])
		}
	}
}

func TestSSTableInvalidFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "invalid.sst")

	// Create an invalid file
	os.WriteFile(path, []byte("not a valid sstable"), 0644)

	_, err := OpenSSTable(path, nil)
	if err == nil {
		t.Error("Expected error opening invalid SSTable")
	}
}

func TestSSTableEmptyFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "empty.sst")

	// Create empty SSTable
	writer, err := NewSSTableWriter(path, nil)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}
	if err := writer.Finish(); err != nil {
		t.Fatalf("Failed to finish: %v", err)
	}

	// Open should work
	reader, err := OpenSSTable(path, nil)
	if err != nil {
		t.Fatalf("Failed to open empty SSTable: %v", err)
	}
	defer reader.Close()

	// Lookup should return not found
	_, _, found := reader.Get([]byte("any"))
	if found {
		t.Error("Should not find anything in empty SSTable")
	}

	// Iterator should have no entries
	iter := reader.NewIterator()
	iter.SeekToFirst()
	if iter.Valid() {
		t.Error("Iterator should not be valid for empty SSTable")
	}
}
