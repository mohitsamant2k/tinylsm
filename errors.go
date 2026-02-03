package lsm

import "errors"

var (
	// ErrNotFound is returned when a key doesn't exist
	ErrNotFound = errors.New("key not found")

	// ErrKeyNotFound is an alias for ErrNotFound (deprecated)
	ErrKeyNotFound = ErrNotFound

	// ErrMemtableImmutable is returned when writing to frozen memtable
	ErrMemtableImmutable = errors.New("memtable is immutable")

	// ErrEmptyKey is returned when key is empty
	ErrEmptyKey = errors.New("key cannot be empty")

	// ErrClosed is returned when db is closed
	ErrClosed = errors.New("database is closed")

	// ErrDatabaseClosed is an alias for ErrClosed (deprecated)
	ErrDatabaseClosed = ErrClosed

	// ErrCorruptedData is returned when data is corrupted
	ErrCorruptedData = errors.New("corrupted data")
)
