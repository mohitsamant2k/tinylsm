package lsm


import (
    "bytes"
    "fmt"
    "sync"
    "testing"
)

func TestSkipListPutGet(t *testing.T) {
    sl := NewSkipList()
    
    // Insert
    sl.Put([]byte("banana"), []byte("yellow"))
    sl.Put([]byte("apple"), []byte("red"))
    sl.Put([]byte("cherry"), []byte("red"))
    
    // Get existing key
    val, deleted, found := sl.Get([]byte("apple"))
    if !found {
        t.Fatal("apple not found")
    }
    if deleted {
        t.Fatal("apple should not be deleted")
    }
    if !bytes.Equal(val, []byte("red")) {
        t.Fatalf("Expected 'red', got '%s'", val)
    }
    
    // Get non-existing key
    _, _, found = sl.Get([]byte("grape"))
    if found {
        t.Fatal("grape should not be found")
    }
}

func TestSkipListUpdate(t *testing.T) {
    sl := NewSkipList()
    
    // Insert
    sl.Put([]byte("key"), []byte("value1"))
    
    // Update
    sl.Put([]byte("key"), []byte("value2"))
    
    // Should have new value
    val, _, found := sl.Get([]byte("key"))
    if !found {
        t.Fatal("key not found")
    }
    if !bytes.Equal(val, []byte("value2")) {
        t.Fatalf("Expected 'value2', got '%s'", val)
    }
    
    // Count should still be 1
    if sl.Count() != 1 {
        t.Fatalf("Expected count 1, got %d", sl.Count())
    }
}

func TestSkipListDelete(t *testing.T) {
    sl := NewSkipList()
    
    sl.Put([]byte("key"), []byte("value"))
    sl.Delete([]byte("key"))
    
    val, deleted, found := sl.Get([]byte("key"))
    if !found {
        t.Fatal("key should be found (as tombstone)")
    }
    if !deleted {
        t.Fatal("key should be marked as deleted")
    }
    if val != nil {
        t.Fatal("deleted key should have nil value")
    }
}

func TestSkipListCount(t *testing.T) {
    sl := NewSkipList()
    
    if sl.Count() != 0 {
        t.Fatal("Empty list should have count 0")
    }
    
    sl.Put([]byte("a"), []byte("1"))
    sl.Put([]byte("b"), []byte("2"))
    sl.Put([]byte("c"), []byte("3"))
    
    if sl.Count() != 3 {
        t.Fatalf("Expected 3, got %d", sl.Count())
    }
}

func TestSkipListIterator(t *testing.T) {
    sl := NewSkipList()
    
    // Insert in random order
    sl.Put([]byte("c"), []byte("3"))
    sl.Put([]byte("a"), []byte("1"))
    sl.Put([]byte("b"), []byte("2"))
    sl.Put([]byte("e"), []byte("5"))
    sl.Put([]byte("d"), []byte("4"))
    
    // Iterate - should be in sorted order
    it := sl.NewIterator()
    defer it.Close()
    
    expected := []string{"a", "b", "c", "d", "e"}
    i := 0
    
    for it.SeekToFirst(); it.Valid(); it.Next() {
        if string(it.Key()) != expected[i] {
            t.Errorf("Position %d: expected '%s', got '%s'", i, expected[i], it.Key())
        }
        i++
    }
    
    if i != 5 {
        t.Errorf("Expected 5 entries, iterated %d", i)
    }
}

func TestSkipListIteratorSeek(t *testing.T) {
    sl := NewSkipList()
    
    sl.Put([]byte("a"), []byte("1"))
    sl.Put([]byte("c"), []byte("3"))
    sl.Put([]byte("e"), []byte("5"))
    sl.Put([]byte("g"), []byte("7"))
    
    it := sl.NewIterator()
    defer it.Close()
    
    // Seek to exact key
    it.Seek([]byte("c"))
    if !it.Valid() || string(it.Key()) != "c" {
        t.Fatalf("Seek to 'c' failed, got '%s'", it.Key())
    }
    
    // Seek to non-existing key (should land on next)
    it.Seek([]byte("d"))
    if !it.Valid() || string(it.Key()) != "e" {
        t.Fatalf("Seek to 'd' should land on 'e', got '%s'", it.Key())
    }
    
    // Seek past all keys
    it.Seek([]byte("z"))
    if it.Valid() {
        t.Fatal("Seek past end should be invalid")
    }
}

func TestSkipListIteratorEmpty(t *testing.T) {
    sl := NewSkipList()
    
    it := sl.NewIterator()
    defer it.Close()
    
    it.SeekToFirst()
    if it.Valid() {
        t.Fatal("Empty list iterator should be invalid")
    }
}

func TestSkipListSize(t *testing.T) {
    sl := NewSkipList()
    
    if sl.Size() != 0 {
        t.Fatal("Empty list should have size 0")
    }
    
    sl.Put([]byte("key"), []byte("value"))
    
    if sl.Size() == 0 {
        t.Fatal("Size should be > 0 after insert")
    }
}

func TestSkipListConcurrentWrites(t *testing.T) {
    sl := NewSkipList()
    var wg sync.WaitGroup
    
    // 10 goroutines, each writing 100 keys
    for i := 0; i < 10; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            for j := 0; j < 100; j++ {
                key := []byte(fmt.Sprintf("key-%02d-%03d", id, j))
                val := []byte(fmt.Sprintf("val-%02d-%03d", id, j))
                sl.Put(key, val)
            }
        }(i)
    }
    
    wg.Wait()
    
    // Should have 1000 entries
    if sl.Count() != 1000 {
        t.Fatalf("Expected 1000, got %d", sl.Count())
    }
}

func TestSkipListConcurrentReadsWrites(t *testing.T) {
    sl := NewSkipList()
    var wg sync.WaitGroup
    
    // Pre-populate
    for i := 0; i < 100; i++ {
        sl.Put([]byte(fmt.Sprintf("key-%03d", i)), []byte("value"))
    }
    
    // 5 writers
    for i := 0; i < 5; i++ {
        wg.Add(1)
        go func(id int) {
            defer wg.Done()
            for j := 0; j < 100; j++ {
                key := []byte(fmt.Sprintf("new-%02d-%03d", id, j))
                sl.Put(key, []byte("value"))
            }
        }(i)
    }
    
    // 5 readers
    for i := 0; i < 5; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for j := 0; j < 100; j++ {
                key := []byte(fmt.Sprintf("key-%03d", j))
                sl.Get(key)
            }
        }()
    }
    
    wg.Wait()
    
    // Should have 100 + 500 = 600 entries
    if sl.Count() != 600 {
        t.Fatalf("Expected 600, got %d", sl.Count())
    }
}