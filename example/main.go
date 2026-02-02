package main

import (
	"fmt"
	"log"
	"os"

	tinylsm "github.com/mohitsamant/tinylsm"
)

func main() {
	// Create a temporary directory for the database
	dbPath := "./testdb"
	defer os.RemoveAll(dbPath)

	// Example 1: Basic Put and Get operations
	fmt.Println("=== Example 1: Basic Put and Get ===")
	basicPutGet(dbPath)

	// Clean up for next example
	os.RemoveAll(dbPath)

	// Example 2: Delete operations
	fmt.Println("\n=== Example 2: Delete Operations ===")
	deleteOperations(dbPath)

	// Clean up for next example
	os.RemoveAll(dbPath)

	// Example 3: Persistence and Recovery
	fmt.Println("\n=== Example 3: Persistence and Recovery ===")
	persistenceExample(dbPath)

	// Clean up for next example
	os.RemoveAll(dbPath)

	// Example 4: Manual Flush to SSTable
	fmt.Println("\n=== Example 4: Manual Flush ===")
	manualFlushExample(dbPath)

	// Clean up for next example
	os.RemoveAll(dbPath)

	// Example 5: Database Statistics
	fmt.Println("\n=== Example 5: Database Statistics ===")
	statsExample(dbPath)
}

// basicPutGet demonstrates basic key-value operations
func basicPutGet(dbPath string) {
	// Open the database with default options
	db, err := tinylsm.Open(tinylsm.DefaultOptions(dbPath))
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Put some key-value pairs
	keys := []string{"name", "age", "city", "country"}
	values := []string{"Alice", "30", "New York", "USA"}

	for i, key := range keys {
		if err := db.Put([]byte(key), []byte(values[i])); err != nil {
			log.Fatalf("Failed to put key %s: %v", key, err)
		}
		fmt.Printf("Put: %s = %s\n", key, values[i])
	}

	// Get the values back
	fmt.Println("\nRetrieving values:")
	for _, key := range keys {
		value, err := db.Get([]byte(key))
		if err != nil {
			log.Fatalf("Failed to get key %s: %v", key, err)
		}
		fmt.Printf("Get: %s = %s\n", key, string(value))
	}

	// Try to get a non-existent key
	_, err = db.Get([]byte("nonexistent"))
	if err == tinylsm.ErrKeyNotFound {
		fmt.Println("\nKey 'nonexistent' not found (expected)")
	}
}

// deleteOperations demonstrates delete functionality
func deleteOperations(dbPath string) {
	db, err := tinylsm.Open(tinylsm.DefaultOptions(dbPath))
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Put a key
	key := []byte("temporary")
	value := []byte("this will be deleted")

	if err := db.Put(key, value); err != nil {
		log.Fatalf("Failed to put key: %v", err)
	}
	fmt.Printf("Put: %s = %s\n", string(key), string(value))

	// Verify it exists
	retrieved, err := db.Get(key)
	if err != nil {
		log.Fatalf("Failed to get key: %v", err)
	}
	fmt.Printf("Get: %s = %s\n", string(key), string(retrieved))

	// Delete the key
	if err := db.Delete(key); err != nil {
		log.Fatalf("Failed to delete key: %v", err)
	}
	fmt.Printf("Deleted: %s\n", string(key))

	// Verify it's gone
	_, err = db.Get(key)
	if err == tinylsm.ErrKeyNotFound {
		fmt.Println("Key successfully deleted (not found)")
	}
}

// persistenceExample demonstrates data persistence across restarts
func persistenceExample(dbPath string) {
	// Open database and write data
	db, err := tinylsm.Open(tinylsm.DefaultOptions(dbPath))
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}

	// Write some data
	if err := db.Put([]byte("persistent_key"), []byte("persistent_value")); err != nil {
		log.Fatalf("Failed to put key: %v", err)
	}
	fmt.Println("Written: persistent_key = persistent_value")

	// Close the database
	if err := db.Close(); err != nil {
		log.Fatalf("Failed to close database: %v", err)
	}
	fmt.Println("Database closed")

	// Reopen the database
	db, err = tinylsm.Open(tinylsm.DefaultOptions(dbPath))
	if err != nil {
		log.Fatalf("Failed to reopen database: %v", err)
	}
	defer db.Close()
	fmt.Println("Database reopened")

	// Verify data is still there
	value, err := db.Get([]byte("persistent_key"))
	if err != nil {
		log.Fatalf("Failed to get key after reopen: %v", err)
	}
	fmt.Printf("Retrieved after reopen: persistent_key = %s\n", string(value))
}

// manualFlushExample demonstrates automatic flushing when memtable is full
func manualFlushExample(dbPath string) {
	opts := tinylsm.DefaultOptions(dbPath)
	opts.MemtableSize = 1024 // Small memtable to trigger automatic flush

	db, err := tinylsm.Open(opts)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Write enough data to trigger automatic flush to SSTable
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("key_%03d", i)
		value := fmt.Sprintf("value_%03d", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			log.Fatalf("Failed to put key: %v", err)
		}
	}
	fmt.Println("Written 200 key-value pairs (automatic flush triggered)")

	// Check stats to see SSTables were created
	stats := db.Stats()
	fmt.Printf("SSTable count: %d\n", stats.SSTableCount)

	// Verify data is still accessible
	value, err := db.Get([]byte("key_050"))
	if err != nil {
		log.Fatalf("Failed to get key after flush: %v", err)
	}
	fmt.Printf("Retrieved after flush: key_050 = %s\n", string(value))
}

// statsExample demonstrates getting database statistics
func statsExample(dbPath string) {
	opts := tinylsm.DefaultOptions(dbPath)
	opts.MemtableSize = 1024 // Small memtable for demonstration

	db, err := tinylsm.Open(opts)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	// Write some data to create SSTables
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("stats_key_%05d", i)
		value := fmt.Sprintf("stats_value_%05d", i)
		if err := db.Put([]byte(key), []byte(value)); err != nil {
			log.Fatalf("Failed to put key: %v", err)
		}
	}

	// Get statistics
	stats := db.Stats()
	fmt.Printf("Database Statistics:\n")
	fmt.Printf("  Memtable Size: %d bytes\n", stats.MemtableSize)
	fmt.Printf("  SSTable Count: %d\n", stats.SSTableCount)
	fmt.Printf("  Disk Usage: %d bytes\n", stats.TotalDiskUsage)
}
