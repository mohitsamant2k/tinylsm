package lsm

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"
)

// WAL record types
const (
	RecordTypePut    byte = 1
	RecordTypeDelete byte = 2
)

// Magic bytes to identify record start (helps recover from corruption)
var walMagic = []byte{0xDE, 0xAD, 0xBE, 0xEF}

// WAL is a write-ahead log for durability
type WAL struct {
	file     *os.File
	writer   *bufio.Writer
	path     string
	mu       sync.Mutex
	syncMode bool // if true, sync to disk on every write
}

// OpenWAL opens or creates a WAL file
// If sync is true, every write is synced to disk (slower but durable)
// If sync is false, writes are buffered (faster but may lose data on crash)
func OpenWAL(path string, sync bool) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	return &WAL{
		file:     file,
		writer:   bufio.NewWriter(file),
		path:     path,
		syncMode: sync,
	}, nil
}

// Write writes a record to the WAL
// Format: [magic:4][recordLen:4][type:1][keyLen:4][valueLen:4][key][value][crc:4]
func (w *WAL) Write(recordType byte, key, value []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Build record
	keyLen := uint32(len(key))
	valueLen := uint32(len(value))

	// Calculate record length (everything after magic+recordLen)
	// type(1) + keyLen(4) + valueLen(4) + key + value + crc(4)
	recordLen := uint32(1 + 4 + 4 + len(key) + len(value) + 4)

	// Calculate CRC of the data
	crc := crc32.NewIEEE()
	crc.Write([]byte{recordType})
	binary.Write(crc, binary.LittleEndian, keyLen)
	binary.Write(crc, binary.LittleEndian, valueLen)
	crc.Write(key)
	crc.Write(value)
	checksum := crc.Sum32()

	// Write magic bytes first (allows scanning for next record if corrupted)
	if _, err := w.writer.Write(walMagic); err != nil {
		return err
	}
	// Write record length (allows skipping corrupted records)
	if err := binary.Write(w.writer, binary.LittleEndian, recordLen); err != nil {
		return err
	}
	// Write record
	if err := w.writer.WriteByte(recordType); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, keyLen); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, valueLen); err != nil {
		return err
	}
	if _, err := w.writer.Write(key); err != nil {
		return err
	}
	if _, err := w.writer.Write(value); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, checksum); err != nil {
		return err
	}

	// Flush to OS buffer
	if err := w.writer.Flush(); err != nil {
		return err
	}

	// If sync mode, also sync to disk for durability
	if w.syncMode {
		return w.file.Sync()
	}

	return nil
}

// WritePut writes a Put record
func (w *WAL) WritePut(key, value []byte) error {
	return w.Write(RecordTypePut, key, value)
}

// WriteDelete writes a Delete record
func (w *WAL) WriteDelete(key []byte) error {
	return w.Write(RecordTypeDelete, key, nil)
}

// Sync forces data to disk
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Sync()
}

// Close closes the WAL
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.writer.Flush(); err != nil {
		return err
	}
	return w.file.Close()
}

// Path returns the WAL file path
func (w *WAL) Path() string {
	return w.path
}

// WALReader reads records from a WAL file
type WALReader struct {
	reader *bufio.Reader
	file   *os.File
}

// NewWALReader creates a reader for WAL recovery
func NewWALReader(path string) (*WALReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	return &WALReader{
		reader: bufio.NewReader(file),
		file:   file,
	}, nil
}

// ReadRecord reads the next record from WAL
// Returns: (recordType, key, value, error)
// Returns io.EOF when no more records
// Returns ErrCorrupted if record is corrupted (caller can try to skip)
func (r *WALReader) ReadRecord() (byte, []byte, []byte, error) {
	// Read and verify magic bytes
	magic := make([]byte, 4)
	if _, err := io.ReadFull(r.reader, magic); err != nil {
		return 0, nil, nil, err
	}

	// Check magic - if it doesn't match, record start is corrupted
	if magic[0] != walMagic[0] || magic[1] != walMagic[1] ||
		magic[2] != walMagic[2] || magic[3] != walMagic[3] {
		return 0, nil, nil, fmt.Errorf("invalid magic bytes: corrupted record start")
	}

	// Read record length
	var recordLen uint32
	if err := binary.Read(r.reader, binary.LittleEndian, &recordLen); err != nil {
		return 0, nil, nil, err
	}

	// Sanity check: record shouldn't be too large (max 100MB)
	if recordLen > 100*1024*1024 {
		return 0, nil, nil, fmt.Errorf("record too large: %d bytes (likely corrupted)", recordLen)
	}

	// Read the entire record
	recordData := make([]byte, recordLen)
	if _, err := io.ReadFull(r.reader, recordData); err != nil {
		return 0, nil, nil, err
	}

	// Parse record from buffer
	if len(recordData) < 1+4+4+4 { // type + keyLen + valueLen + crc minimum
		return 0, nil, nil, fmt.Errorf("record too short")
	}

	recordType := recordData[0]
	keyLen := binary.LittleEndian.Uint32(recordData[1:5])
	valueLen := binary.LittleEndian.Uint32(recordData[5:9])

	// Validate lengths
	expectedLen := uint32(1 + 4 + 4 + keyLen + valueLen + 4)
	if recordLen != expectedLen {
		return 0, nil, nil, fmt.Errorf("record length mismatch")
	}

	// Extract key and value
	keyStart := uint32(9)
	key := recordData[keyStart : keyStart+keyLen]
	valueStart := keyStart + keyLen
	value := recordData[valueStart : valueStart+valueLen]

	// Extract and verify CRC
	crcStart := valueStart + valueLen
	storedCRC := binary.LittleEndian.Uint32(recordData[crcStart:])

	// Calculate expected CRC
	crc := crc32.NewIEEE()
	crc.Write([]byte{recordType})
	binary.Write(crc, binary.LittleEndian, keyLen)
	binary.Write(crc, binary.LittleEndian, valueLen)
	crc.Write(key)
	crc.Write(value)

	if crc.Sum32() != storedCRC {
		return 0, nil, nil, fmt.Errorf("CRC mismatch: corrupted record")
	}

	return recordType, key, value, nil
}

// Close closes the reader
func (r *WALReader) Close() error {
	return r.file.Close()
}

// ScanToNextRecord scans forward looking for the next magic bytes
// Used to recover from corruption by finding the next valid record
// Returns true if found, false if EOF reached
func (r *WALReader) ScanToNextRecord() bool {
	// We look for the magic sequence byte by byte
	matchCount := 0

	for {
		b, err := r.reader.ReadByte()
		if err != nil {
			return false // EOF or error
		}

		switch b {
		case walMagic[matchCount]:
			matchCount++
			if matchCount == 4 {
				// Found complete magic sequence!
				// Unread the last byte so we can unread all 4
				r.reader.UnreadByte()

				// Now we're positioned right after the first 3 magic bytes
				// We need to "unread" the magic so ReadRecord can find it
				// Discard buffered data and seek back in file
				buffered := r.reader.Buffered()
				currentPos, _ := r.file.Seek(0, io.SeekCurrent)
				// Position = file position - buffered - 3 (for the 3 magic bytes we consumed)
				newPos := currentPos - int64(buffered) - 3
				r.file.Seek(newPos, io.SeekStart)
				r.reader.Reset(r.file)
				return true
			}
		case walMagic[0]:
			// Might be start of a new magic sequence
			matchCount = 1
		default:
			matchCount = 0
		}
	}
}

// RecoverMemtable rebuilds a memtable from WAL
// Skips corrupted records by scanning for next magic bytes
func RecoverMemtable(walPath string, maxSize int64) (*Memtable, error) {
	reader, err := NewWALReader(walPath)
	if err != nil {
		if os.IsNotExist(err) {
			return NewMemtable(maxSize), nil // No WAL, fresh start
		}
		return nil, err
	}
	defer reader.Close()

	mem := NewMemtable(maxSize)
	recovered := 0
	corrupted := 0

	for {
		recordType, key, value, err := reader.ReadRecord()

		if err == io.EOF {
			break // Normal end of file
		}

		if err != nil {
			// Corrupted record - scan forward to find next valid record
			corrupted++
			if !reader.ScanToNextRecord() {
				break // No more valid records found
			}
			continue // Try reading the record we found
		}

		switch recordType {
		case RecordTypePut:
			mem.data.Put(key, value)
			recovered++
		case RecordTypeDelete:
			mem.data.Delete(key)
			recovered++
		}
	}

	if corrupted > 0 {
		fmt.Printf("WAL Recovery: %d records recovered, %d corrupted records skipped\n",
			recovered, corrupted)
	}

	return mem, nil
}
