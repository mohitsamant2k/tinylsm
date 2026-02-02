package lsm

import (
    "bytes"
    "io"
    "os"
    "path/filepath"
    "testing"
)

func TestWALWriteRead(t *testing.T) {
    dir := t.TempDir()
    walPath := filepath.Join(dir, "test.wal")

    // Open with sync=false for faster tests
    wal, err := OpenWAL(walPath, false)
    if err != nil {
        t.Fatalf("Failed to open WAL: %v", err)
    }

    wal.WritePut([]byte("key1"), []byte("value1"))
    wal.WritePut([]byte("key2"), []byte("value2"))
    wal.WriteDelete([]byte("key1"))
    wal.Close()

    // Read records back
    reader, err := NewWALReader(walPath)
    if err != nil {
        t.Fatalf("Failed to open WAL reader: %v", err)
    }
    defer reader.Close()

    // Record 1: Put key1
    recType, key, _, err := reader.ReadRecord()
    if err != nil {
        t.Fatalf("Failed to read record 1: %v", err)
    }
    if recType != RecordTypePut || !bytes.Equal(key, []byte("key1")) {
        t.Fatal("Record 1 mismatch")
    }

    // Record 2: Put key2
    recType, key, value, err := reader.ReadRecord()
    if err != nil {
        t.Fatalf("Failed to read record 2: %v", err)
    }
    if recType != RecordTypePut || !bytes.Equal(key, []byte("key2")) {
        t.Fatal("Record 2 mismatch")
    }
    if !bytes.Equal(value, []byte("value2")) {
        t.Fatal("Record 2 value mismatch")
    }

    // Record 3: Delete key1
    recType, key, _, err = reader.ReadRecord()
    if err != nil {
        t.Fatalf("Failed to read record 3: %v", err)
    }
    if recType != RecordTypeDelete || !bytes.Equal(key, []byte("key1")) {
        t.Fatal("Record 3 mismatch")
    }

    // EOF
    _, _, _, err = reader.ReadRecord()
    if err != io.EOF {
        t.Fatal("Expected EOF")
    }
}

func TestWALRecovery(t *testing.T) {
    dir := t.TempDir()
    walPath := filepath.Join(dir, "test.wal")

    // Simulate writes before crash
    wal, _ := OpenWAL(walPath, false)
    wal.WritePut([]byte("name"), []byte("Alice"))
    wal.WritePut([]byte("age"), []byte("30"))
    wal.WriteDelete([]byte("age"))
    wal.WritePut([]byte("city"), []byte("NYC"))
    wal.Close()

    // Recover memtable from WAL
    mem, err := RecoverMemtable(walPath, 1024*1024)
    if err != nil {
        t.Fatalf("Recovery failed: %v", err)
    }

    // Verify recovered data
    val, deleted, found := mem.Get([]byte("name"))
    if !found || deleted || !bytes.Equal(val, []byte("Alice")) {
        t.Fatal("name not recovered correctly")
    }

    _, deleted, found = mem.Get([]byte("age"))
    if !found || !deleted {
        t.Fatal("age should be deleted")
    }

    val, deleted, found = mem.Get([]byte("city"))
    if !found || deleted || !bytes.Equal(val, []byte("NYC")) {
        t.Fatal("city not recovered correctly")
    }
}

func TestWALRecoveryNoFile(t *testing.T) {
    mem, err := RecoverMemtable("/nonexistent/path.wal", 1024*1024)
    if err != nil {
        t.Fatalf("Should not error on missing WAL: %v", err)
    }
    if mem.Count() != 0 {
        t.Fatal("Should be empty memtable")
    }
}

func TestWALWithSync(t *testing.T) {
    dir := t.TempDir()
    walPath := filepath.Join(dir, "test.wal")

    // Open with sync=true (durable)
    wal, _ := OpenWAL(walPath, true)
    wal.WritePut([]byte("key"), []byte("value"))
    wal.Close()

    // Verify file exists and has data
    info, _ := os.Stat(walPath)
    if info.Size() == 0 {
        t.Fatal("WAL file should have data")
    }
}

func TestWALSync(t *testing.T) {
    dir := t.TempDir()
    walPath := filepath.Join(dir, "test.wal")

    wal, _ := OpenWAL(walPath, false)
    wal.WritePut([]byte("key"), []byte("value"))
    
    // Explicit sync
    err := wal.Sync()
    if err != nil {
        t.Fatalf("Sync failed: %v", err)
    }
    
    wal.Close()

    info, _ := os.Stat(walPath)
    if info.Size() == 0 {
        t.Fatal("WAL file should have data after sync")
    }
}