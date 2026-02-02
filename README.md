# TinyLSM

A lightweight Log-Structured Merge Tree (LSM-Tree) key-value storage engine written in Go. TinyLSM is designed for learning and understanding how modern databases like LevelDB, RocksDB, and Cassandra work under the hood.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [How It Works](#how-it-works)
  - [Write Path](#write-path)
  - [Read Path](#read-path)
  - [Components](#components)
- [Installation](#installation)
- [Usage](#usage)
- [API Reference](#api-reference)
- [Configuration](#configuration)
- [File Format](#file-format)
- [Contributing](#contributing)
- [License](#license)

## Features

- **LSM-Tree Architecture**: Write-optimized storage with efficient reads
- **Write-Ahead Log (WAL)**: Durability guarantee for all writes
- **Memtable with Skip List**: Fast in-memory sorted data structure
- **SSTable Storage**: Immutable sorted files on disk with block-based layout
- **Automatic Compaction**: Memtable flushes when size threshold is reached
- **Crash Recovery**: Automatic recovery from WAL on restart
- **Concurrent Access**: Thread-safe reads and writes
- **Tombstone Deletes**: Proper deletion handling across memtable and SSTables

## Architecture

```
                    ┌─────────────────────────────────────┐
                    │              TinyLSM                │
                    └─────────────────────────────────────┘
                                     │
         ┌───────────────────────────┼───────────────────────────┐
         │                           │                           │
         ▼                           ▼                           ▼
   ┌──────────┐               ┌──────────┐               ┌──────────────┐
   │   WAL    │               │ Memtable │               │   SSTables   │
   │(wal.log) │               │(SkipList)│               │ (*.sst files)│
   └──────────┘               └──────────┘               └──────────────┘
         │                           │                           │
         │                           │     flush when full       │
         │                           ├──────────────────────────►│
         │                           │                           │
         │◄──────────────────────────┤                           │
         │    write-ahead logging    │                           │
         │                           │                           │
```

## How It Works

### Write Path

1. **Write to WAL**: Every write (Put/Delete) is first appended to the Write-Ahead Log for durability
2. **Write to Memtable**: The operation is then applied to the in-memory Memtable (a Skip List)
3. **Flush to SSTable**: When Memtable reaches its size limit, it becomes immutable and is flushed to an SSTable on disk
4. **WAL Cleanup**: After successful flush, a new WAL is created for subsequent writes

```
Put("key", "value")
        │
        ▼
  ┌───────────┐
  │ Write WAL │ ──► Durability (survives crash)
  └───────────┘
        │
        ▼
  ┌───────────┐
  │ Write     │
  │ Memtable  │ ──► Fast in-memory write (Skip List)
  └───────────┘
        │
        ▼ (if memtable full)
  ┌───────────┐
  │ Flush to  │
  │ SSTable   │ ──► Persistent sorted storage
  └───────────┘
```

### Read Path

Reads follow a specific order to find the most recent value:

1. **Check Memtable**: Look in the active memtable first (most recent writes)
2. **Check Immutable Memtable**: If a flush is in progress, check the immutable memtable
3. **Search SSTables**: Search SSTables from newest to oldest until the key is found

```
Get("key")
    │
    ▼
┌──────────────┐     found
│   Memtable   │ ─────────────► return value
└──────────────┘
    │ not found
    ▼
┌──────────────┐     found
│  Immutable   │ ─────────────► return value
│   Memtable   │
└──────────────┘
    │ not found
    ▼
┌──────────────┐     found
│   SSTable    │ ─────────────► return value (or tombstone = deleted)
│  (newest)    │
└──────────────┘
    │ not found
    ▼
┌──────────────┐
│   SSTable    │ ─────────────► continue searching older SSTables...
│   (older)    │
└──────────────┘
    │ not found in any
    ▼
  ErrKeyNotFound
```

### Components

#### 1. Skip List (`skiplist.go`)

A probabilistic data structure that provides O(log n) search, insert, and delete operations. Used as the underlying structure for the Memtable.

```
Level 3: HEAD ─────────────────────────────────────► 50 ─────────────────► NIL
Level 2: HEAD ─────────► 20 ─────────────────────► 50 ─────────► 70 ──► NIL
Level 1: HEAD ──► 10 ──► 20 ──► 30 ──► 40 ──► 50 ──► 60 ──► 70 ──► NIL
```

**Key features:**
- Concurrent-safe with read-write mutex
- Configurable max level (default: 12)
- Tracks total size in bytes for flush decisions

#### 2. Memtable (`memtable.go`)

An in-memory buffer for recent writes, backed by a Skip List.

**Key features:**
- Configurable max size (default: 4MB)
- Supports Put, Get, and Delete operations
- Can be marked as immutable during flush
- Provides iterator for sequential access

#### 3. Write-Ahead Log (`wal.go`)

Ensures durability by logging every write before applying it to the memtable.

**Record format:**
```
┌────────┬───────────┬──────┬────────┬──────────┬─────┬───────┬─────┐
│ Magic  │ RecordLen │ Type │ KeyLen │ ValueLen │ Key │ Value │ CRC │
│ 4 bytes│  4 bytes  │1 byte│ 4 bytes│  4 bytes │ var │  var  │4 bytes│
└────────┴───────────┴──────┴────────┴──────────┴─────┴───────┴─────┘
```

**Features:**
- Magic bytes for record boundary detection
- CRC32 checksum for corruption detection
- Supports sync mode for immediate durability
- Recovery can skip corrupted records

#### 4. SSTable (`sstable.go`)

Sorted String Table - immutable, sorted file format for persistent storage.

**File structure:**
```
┌─────────────────────────────────────────────────────────────┐
│                      Data Blocks                            │
│  ┌──────────────────────────────────────────────────────┐   │
│  │ Block 0: [Entry][Entry][Entry]...[Padding][CRC]      │   │
│  ├──────────────────────────────────────────────────────┤   │
│  │ Block 1: [Entry][Entry][Entry]...[Padding][CRC]      │   │
│  ├──────────────────────────────────────────────────────┤   │
│  │ Block N: ...                                          │   │
│  └──────────────────────────────────────────────────────┘   │
├─────────────────────────────────────────────────────────────┤
│                      Index Block                            │
│  [FirstKey₀][Offset₀][Size₀][FirstKey₁][Offset₁][Size₁]... │
├─────────────────────────────────────────────────────────────┤
│                        Footer                               │
│  [IndexOffset:8][IndexSize:8][Magic:8]                      │
└─────────────────────────────────────────────────────────────┘
```

**Features:**
- Block-based layout (4KB blocks, optimized for SSDs)
- Index for efficient key lookups
- CRC32 checksum per block
- Magic number for file validation

## Installation

```bash
go get github.com/mohitsamant2k/tinylsm
```

## Usage

### Basic Example

```go
package main

import (
    "fmt"
    "log"

    tinylsm "github.com/mohitsamant2k/tinylsm"
)

func main() {
    // Open database with default options
    db, err := tinylsm.Open(tinylsm.DefaultOptions("./mydb"))
    if err != nil {
        log.Fatal(err)
    }
    defer db.Close()

    // Put a key-value pair
    err = db.Put([]byte("hello"), []byte("world"))
    if err != nil {
        log.Fatal(err)
    }

    // Get the value
    value, err := db.Get([]byte("hello"))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("hello = %s\n", value) // Output: hello = world

    // Delete a key
    err = db.Delete([]byte("hello"))
    if err != nil {
        log.Fatal(err)
    }

    // Key not found after deletion
    _, err = db.Get([]byte("hello"))
    if err == tinylsm.ErrKeyNotFound {
        fmt.Println("Key deleted successfully")
    }
}
```

### Running the Example

```bash
cd example
go run main.go
```

## API Reference

### Opening a Database

```go
// With default options
db, err := tinylsm.Open(tinylsm.DefaultOptions("/path/to/db"))

// With custom options
opts := &tinylsm.DBOptions{
    Dir:          "/path/to/db",
    MemtableSize: 8 * 1024 * 1024, // 8MB memtable
    SyncWrites:   true,            // Sync every write (slower but durable)
}
db, err := tinylsm.Open(opts)
```

### Operations

```go
// Put a key-value pair
err := db.Put(key []byte, value []byte)

// Get a value by key
value, err := db.Get(key []byte) // Returns ErrKeyNotFound if not found

// Delete a key
err := db.Delete(key []byte)

// Close the database
err := db.Close()

// Get database statistics
stats := db.Stats()
fmt.Printf("Memtable size: %d bytes\n", stats.MemtableSize)
fmt.Printf("SSTable count: %d\n", stats.SSTableCount)
fmt.Printf("Disk usage: %d bytes\n", stats.TotalDiskUsage)
```

### Errors

```go
tinylsm.ErrKeyNotFound   // Key does not exist
tinylsm.ErrDBClosed      // Database has been closed
```

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `Dir` | (required) | Directory to store database files |
| `MemtableSize` | 4MB | Maximum memtable size before flush |
| `SyncWrites` | false | Sync WAL on every write for durability |

## File Format

TinyLSM creates the following files in the database directory:

```
mydb/
├── wal.log           # Write-ahead log for current memtable
├── 000001.sst        # SSTable files (sorted, immutable)
├── 000002.sst
└── 000003.sst
```

## Performance Characteristics

| Operation | Time Complexity | Notes |
|-----------|-----------------|-------|
| Put | O(log n) | Write to WAL + Skip List |
| Get | O(log n) per level | Memtable → Immutable → SSTables |
| Delete | O(log n) | Same as Put (writes tombstone) |

**Trade-offs:**
- **Writes are fast**: Append-only WAL + in-memory Skip List
- **Reads may be slower**: May need to check multiple SSTables
- **Space amplification**: Deleted keys remain until compaction

## Future Improvements

- [ ] **Compaction**: Merge SSTables to reclaim space and improve read performance
- [ ] **Bloom Filters**: Skip SSTables that definitely don't contain a key
- [ ] **Block Cache**: Cache frequently accessed blocks in memory
- [ ] **Compression**: Snappy/LZ4 compression for blocks
- [ ] **Range Queries**: Scan operations with iterators
- [ ] **MVCC**: Multi-version concurrency control for snapshots

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT License - feel free to use this for learning and building your own storage engines!

## References

- [LevelDB Implementation Notes](https://github.com/google/leveldb/blob/main/doc/impl.md)
- [RocksDB Wiki](https://github.com/facebook/rocksdb/wiki)
- [The Log-Structured Merge-Tree (LSM-Tree)](https://www.cs.umb.edu/~pon} 
- [Skip Lists: A Probabilistic Alternative to Balanced Trees](https://15721.courses.cs.cmu.edu/spring2018/papers/08-oltpindexes1/pugh-skiplists-cacm1990.pdf)
