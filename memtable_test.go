package lsm

import (
	"bytes"
	"fmt"
	"testing"
)

func TestMemtablePutGet(t *testing.T) {
	mem := NewMemtable(1024 * 1024) // 1MB

	err := mem.Put([]byte("name"), []byte("Alice"))
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	val, deleted, found := mem.Get([]byte("name")) // (value, deleted, found)
	if !found {
		t.Fatal("Key not found")
	}
	if deleted {
		t.Fatal("Key should not be deleted")
	}
	if !bytes.Equal(val, []byte("Alice")) {
		t.Fatalf("Expected 'Alice', got '%s'", val)
	}
}

func TestMemtableDelete(t *testing.T) {
	mem := NewMemtable(1024 * 1024)

	mem.Put([]byte("key"), []byte("value"))
	mem.Delete([]byte("key"))

	_, deleted, found := mem.Get([]byte("key")) // (value, deleted, found)
	if !found {
		t.Fatal("Key should be found (as tombstone)")
	}
	if !deleted {
		t.Fatal("Key should be marked deleted")
	}
}

func TestMemtableIsFull(t *testing.T) {
	mem := NewMemtable(100) // 100 bytes

	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key%d", i))
		value := []byte(fmt.Sprintf("value%d", i))
		mem.Put(key, value)
	}

	if !mem.IsFull() {
		t.Fatal("Memtable should be full")
	}
}

func TestMemtableImmutable(t *testing.T) {
	mem := NewMemtable(1024 * 1024)

	mem.Put([]byte("key1"), []byte("value1"))
	mem.SetImmutable()

	// Write should fail
	err := mem.Put([]byte("key2"), []byte("value2"))
	if err != ErrMemtableImmutable {
		t.Fatalf("Expected ErrMemtableImmutable, got %v", err)
	}

	// Read should work
	val, _, found := mem.Get([]byte("key1")) // (value, deleted, found)
	if !found || !bytes.Equal(val, []byte("value1")) {
		t.Fatal("Read from immutable failed")
	}
}

func TestMemtableIterator(t *testing.T) {
	mem := NewMemtable(1024 * 1024)

	mem.Put([]byte("c"), []byte("3"))
	mem.Put([]byte("a"), []byte("1"))
	mem.Put([]byte("b"), []byte("2"))

	it := mem.NewIterator()
	defer it.Close()

	expected := []string{"a", "b", "c"}
	i := 0

	for it.SeekToFirst(); it.Valid(); it.Next() {
		if string(it.Key()) != expected[i] {
			t.Errorf("Expected %s, got %s", expected[i], it.Key())
		}
		i++
	}
}
