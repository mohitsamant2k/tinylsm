package lsm

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sort"
)

const (
	// Block size target (4KB is common, good for SSD page size)
	BlockSize = 4 * 1024

	// Magic number for SSTable footer validation
	SSTableMagic uint64 = 0x53535461626C6521 // "SSTable!" in hex
)

// BlockHandle points to a block in the file
type BlockHandle struct {
	Offset uint64 // Where the block starts
	Size   uint64 // Block size in bytes
}

// IndexEntry maps a key to its block
type IndexEntry struct {
	FirstKey []byte      // First key in the block
	Handle   BlockHandle // Where to find the block
}

// SSTableWriter writes a new SSTable file
type SSTableWriter struct {
	file        *os.File
	writer      *bufio.Writer
	offset      uint64       // Current write position
	blockBuffer bytes.Buffer // Buffer for current data block
	index       []IndexEntry // Index entries for all blocks
	firstKey    []byte       // First key of current block
	entryCount  int          // Entries in current block
	totalKeys   int          // Total keys added (for bloom filter sizing)
	bloomFilter *BloomFilter // Bloom filter for fast negative lookups
	bitsPerKey  int          // Bits per key for bloom filter
	comparator  Comparator
}

// NewSSTableWriter creates a writer for a new SSTable
// bitsPerKey controls bloom filter size (0 = no bloom filter)
func NewSSTableWriter(path string, comparator Comparator, bitsPerKey int) (*SSTableWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("failed to create SSTable: %w", err)
	}

	if comparator == nil {
		comparator = DefaultComparator{}
	}

	return &SSTableWriter{
		file:        file,
		writer:      bufio.NewWriter(file),
		comparator:  comparator,
		index:       make([]IndexEntry, 0),
		bloomFilter: nil, // Will be created lazily when we know the size
		bitsPerKey:  bitsPerKey,
	}, nil
}

// Add adds a key-value pair (must be called in sorted order!)
func (w *SSTableWriter) Add(key, value []byte, deleted bool) error {
	// Track total keys for bloom filter
	w.totalKeys++

	// Add key to bloom filter (lazy initialization, skip if bitsPerKey is 0)
	if w.bitsPerKey > 0 {
		if w.bloomFilter == nil {
			// Estimate: start with 1000 keys
			w.bloomFilter = NewBloomFilter(1000, w.bitsPerKey)
		}
		w.bloomFilter.Add(key)
	}

	// Remember first key of block
	if w.entryCount == 0 {
		w.firstKey = make([]byte, len(key))
		copy(w.firstKey, key)
	}

	// Encode entry into block buffer
	// Format: [keyLen:4][valueLen:4][deleted:1][key][value]
	if err := binary.Write(&w.blockBuffer, binary.LittleEndian, uint32(len(key))); err != nil {
		return err
	}
	if err := binary.Write(&w.blockBuffer, binary.LittleEndian, uint32(len(value))); err != nil {
		return err
	}
	deletedByte := byte(0)
	if deleted {
		deletedByte = 1
	}
	w.blockBuffer.WriteByte(deletedByte)
	w.blockBuffer.Write(key)
	w.blockBuffer.Write(value)

	w.entryCount++

	// Flush block if it's big enough
	if w.blockBuffer.Len() >= BlockSize {
		return w.flushBlock()
	}

	return nil
}

// flushBlock writes the current block to file
func (w *SSTableWriter) flushBlock() error {
	if w.entryCount == 0 {
		return nil // Nothing to flush
	}

	blockData := w.blockBuffer.Bytes()

	// Calculate CRC for the block
	crc := crc32.ChecksumIEEE(blockData)

	// Record index entry
	w.index = append(w.index, IndexEntry{
		FirstKey: w.firstKey,
		Handle: BlockHandle{
			Offset: w.offset,
			Size:   uint64(len(blockData) + 4), // +4 for CRC
		},
	})

	// Write block data
	if _, err := w.writer.Write(blockData); err != nil {
		return err
	}
	// Write block CRC
	if err := binary.Write(w.writer, binary.LittleEndian, crc); err != nil {
		return err
	}

	w.offset += uint64(len(blockData) + 4)

	// Reset for next block
	w.blockBuffer.Reset()
	w.firstKey = nil
	w.entryCount = 0

	return nil
}

// Finish completes the SSTable and writes index + footer
func (w *SSTableWriter) Finish() error {
	// Flush any remaining data block
	if err := w.flushBlock(); err != nil {
		return err
	}

	// Write index block
	indexOffset := w.offset

	// Encode index entries
	// Format: [numEntries:4] followed by entries
	if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(w.index))); err != nil {
		return err
	}
	w.offset += 4

	for _, entry := range w.index {
		// [keyLen:4][key][offset:8][size:8]
		if err := binary.Write(w.writer, binary.LittleEndian, uint32(len(entry.FirstKey))); err != nil {
			return err
		}
		if _, err := w.writer.Write(entry.FirstKey); err != nil {
			return err
		}
		if err := binary.Write(w.writer, binary.LittleEndian, entry.Handle.Offset); err != nil {
			return err
		}
		if err := binary.Write(w.writer, binary.LittleEndian, entry.Handle.Size); err != nil {
			return err
		}
		w.offset += uint64(4 + len(entry.FirstKey) + 8 + 8)
	}

	indexSize := w.offset - indexOffset

	// Write bloom filter block
	bloomOffset := w.offset
	var bloomSize uint64 = 0
	if w.bloomFilter != nil {
		bloomData := w.bloomFilter.Encode()
		if _, err := w.writer.Write(bloomData); err != nil {
			return err
		}
		bloomSize = uint64(len(bloomData))
		w.offset += bloomSize
	}

	// Write footer
	// [indexOffset:8][indexSize:8][bloomOffset:8][bloomSize:8][magic:8]
	if err := binary.Write(w.writer, binary.LittleEndian, indexOffset); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, indexSize); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, bloomOffset); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, bloomSize); err != nil {
		return err
	}
	if err := binary.Write(w.writer, binary.LittleEndian, SSTableMagic); err != nil {
		return err
	}

	// Flush and sync
	if err := w.writer.Flush(); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}

	return w.file.Close()
}

// Close closes the writer without finishing (for error cases)
func (w *SSTableWriter) Close() error {
	return w.file.Close()
}

// SSTableReader reads from an SSTable file
type SSTableReader struct {
	file        *os.File
	size        int64
	index       []IndexEntry
	bloomFilter *BloomFilter // Bloom filter for fast negative lookups
	comparator  Comparator
	path        string
}

// OpenSSTable opens an existing SSTable for reading
func OpenSSTable(path string, comparator Comparator) (*SSTableReader, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	// Get file size
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, err
	}

	if comparator == nil {
		comparator = DefaultComparator{}
	}

	r := &SSTableReader{
		file:       file,
		size:       stat.Size(),
		comparator: comparator,
		path:       path,
	}

	// Read and validate footer
	if err := r.readFooter(); err != nil {
		file.Close()
		return nil, err
	}

	return r, nil
}

// readFooter reads the footer and index
func (r *SSTableReader) readFooter() error {
	// Try new footer format first: 40 bytes
	// [indexOffset:8][indexSize:8][bloomOffset:8][bloomSize:8][magic:8]
	if r.size >= 40 {
		footer := make([]byte, 40)
		if _, err := r.file.ReadAt(footer, r.size-40); err != nil {
			return err
		}

		magic := binary.LittleEndian.Uint64(footer[32:40])
		if magic == SSTableMagic {
			// New format with bloom filter
			indexOffset := binary.LittleEndian.Uint64(footer[0:8])
			indexSize := binary.LittleEndian.Uint64(footer[8:16])
			bloomOffset := binary.LittleEndian.Uint64(footer[16:24])
			bloomSize := binary.LittleEndian.Uint64(footer[24:32])

			// Read bloom filter if present
			if bloomSize > 0 {
				bloomData := make([]byte, bloomSize)
				if _, err := r.file.ReadAt(bloomData, int64(bloomOffset)); err != nil {
					return err
				}
				bf, err := DecodeBloomFilter(bloomData)
				if err != nil {
					return fmt.Errorf("failed to decode bloom filter: %w", err)
				}
				r.bloomFilter = bf
			}

			return r.readIndex(indexOffset, indexSize)
		}
	}

	// Fall back to old footer format: 24 bytes (for backward compatibility)
	// [indexOffset:8][indexSize:8][magic:8]
	if r.size < 24 {
		return fmt.Errorf("SSTable too small")
	}

	footer := make([]byte, 24)
	if _, err := r.file.ReadAt(footer, r.size-24); err != nil {
		return err
	}

	indexOffset := binary.LittleEndian.Uint64(footer[0:8])
	indexSize := binary.LittleEndian.Uint64(footer[8:16])
	magic := binary.LittleEndian.Uint64(footer[16:24])

	if magic != SSTableMagic {
		return fmt.Errorf("invalid SSTable: bad magic number")
	}

	return r.readIndex(indexOffset, indexSize)
}

// readIndex reads the index block
func (r *SSTableReader) readIndex(indexOffset, indexSize uint64) error {

	// Read index block
	indexData := make([]byte, indexSize)
	if _, err := r.file.ReadAt(indexData, int64(indexOffset)); err != nil {
		return err
	}

	// Parse index
	reader := bytes.NewReader(indexData)
	var numEntries uint32
	if err := binary.Read(reader, binary.LittleEndian, &numEntries); err != nil {
		return err
	}

	r.index = make([]IndexEntry, numEntries)
	for i := uint32(0); i < numEntries; i++ {
		var keyLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			return err
		}
		key := make([]byte, keyLen)
		if _, err := reader.Read(key); err != nil {
			return err
		}
		var offset, size uint64
		if err := binary.Read(reader, binary.LittleEndian, &offset); err != nil {
			return err
		}
		if err := binary.Read(reader, binary.LittleEndian, &size); err != nil {
			return err
		}
		r.index[i] = IndexEntry{
			FirstKey: key,
			Handle:   BlockHandle{Offset: offset, Size: size},
		}
	}

	return nil
}

// MayContain checks if a key might be in the SSTable using the bloom filter.
// Returns true if the key might exist, false if it definitely doesn't.
// If no bloom filter is present, always returns true (conservative).
func (r *SSTableReader) MayContain(key []byte) bool {
	if r.bloomFilter == nil {
		return true // No bloom filter, must check SSTable
	}
	return r.bloomFilter.MayContain(key)
}

// Get looks up a key in the SSTable
// Returns: (value, deleted, found)
func (r *SSTableReader) Get(key []byte) ([]byte, bool, bool) {
	// Find which block might contain the key using index
	blockIdx := r.findBlock(key)
	if blockIdx < 0 {
		return nil, false, false
	}

	// Read and search the block
	return r.searchBlock(blockIdx, key)
}

// findBlock finds which block might contain the key
// Uses binary search on the index
func (r *SSTableReader) findBlock(key []byte) int {
	if len(r.index) == 0 {
		return -1
	}

	// Binary search: find the last block where firstKey <= key
	idx := sort.Search(len(r.index), func(i int) bool {
		return r.comparator.Compare(r.index[i].FirstKey, key) > 0
	})

	// idx is the first block where firstKey > key
	// So we want idx - 1
	if idx == 0 {
		// Key is smaller than first key of first block
		// But it might still be in first block (if there's only one block)
		// Actually no - if key < firstKey of block 0, it's not in the table
		return -1
	}

	return idx - 1
}

// searchBlock reads a block and searches for the key
func (r *SSTableReader) searchBlock(blockIdx int, key []byte) ([]byte, bool, bool) {
	handle := r.index[blockIdx].Handle

	// Read block (excluding CRC)
	blockData := make([]byte, handle.Size)
	if _, err := r.file.ReadAt(blockData, int64(handle.Offset)); err != nil {
		return nil, false, false
	}

	// Verify CRC
	dataPart := blockData[:len(blockData)-4]
	storedCRC := binary.LittleEndian.Uint32(blockData[len(blockData)-4:])
	if crc32.ChecksumIEEE(dataPart) != storedCRC {
		return nil, false, false // Corrupted block
	}

	// Search through entries
	reader := bytes.NewReader(dataPart)
	for reader.Len() > 0 {
		var keyLen, valueLen uint32
		if err := binary.Read(reader, binary.LittleEndian, &keyLen); err != nil {
			break
		}
		if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
			break
		}
		deletedByte, err := reader.ReadByte()
		if err != nil {
			break
		}

		entryKey := make([]byte, keyLen)
		if _, err := reader.Read(entryKey); err != nil {
			break
		}
		entryValue := make([]byte, valueLen)
		if _, err := reader.Read(entryValue); err != nil {
			break
		}

		cmp := r.comparator.Compare(entryKey, key)
		if cmp == 0 {
			// Found it!
			return entryValue, deletedByte == 1, true
		}
		if cmp > 0 {
			// Passed where key would be (keys are sorted)
			break
		}
	}

	return nil, false, false
}

// Close closes the SSTable
func (r *SSTableReader) Close() error {
	return r.file.Close()
}

// Path returns the file path
func (r *SSTableReader) Path() string {
	return r.path
}

// NewIterator returns an iterator over all entries
func (r *SSTableReader) NewIterator() *SSTableIterator {
	return &SSTableIterator{
		reader:      r,
		blockIdx:    0,
		blockData:   nil,
		blockReader: nil,
	}
}

// SSTableIterator iterates over SSTable entries
type SSTableIterator struct {
	reader      *SSTableReader
	blockIdx    int
	blockData   []byte
	blockReader *bytes.Reader

	// Current entry
	key     []byte
	value   []byte
	deleted bool
	valid   bool
}

// SeekToFirst positions at the first entry
func (it *SSTableIterator) SeekToFirst() {
	it.blockIdx = -1 // Start before first block, Next() will increment to 0
	it.blockData = nil
	it.blockReader = nil
	it.valid = false
	it.Next()
}

// loadBlock loads the current block
func (it *SSTableIterator) loadBlock() bool {
	if it.blockIdx >= len(it.reader.index) {
		it.valid = false
		return false
	}

	handle := it.reader.index[it.blockIdx].Handle
	it.blockData = make([]byte, handle.Size)
	if _, err := it.reader.file.ReadAt(it.blockData, int64(handle.Offset)); err != nil {
		it.valid = false
		return false
	}

	// Verify CRC
	dataPart := it.blockData[:len(it.blockData)-4]
	storedCRC := binary.LittleEndian.Uint32(it.blockData[len(it.blockData)-4:])
	if crc32.ChecksumIEEE(dataPart) != storedCRC {
		it.valid = false
		return false
	}

	it.blockReader = bytes.NewReader(dataPart)
	return true
}

// Next advances to the next entry
func (it *SSTableIterator) Next() {
	for {
		if it.blockReader == nil || it.blockReader.Len() == 0 {
			// Need next block
			it.blockIdx++
			if !it.loadBlock() {
				it.valid = false
				return
			}
		}

		// Read next entry from block
		var keyLen, valueLen uint32
		if err := binary.Read(it.blockReader, binary.LittleEndian, &keyLen); err != nil {
			it.valid = false
			return
		}
		if err := binary.Read(it.blockReader, binary.LittleEndian, &valueLen); err != nil {
			it.valid = false
			return
		}
		deletedByte, err := it.blockReader.ReadByte()
		if err != nil {
			it.valid = false
			return
		}

		it.key = make([]byte, keyLen)
		if _, err := it.blockReader.Read(it.key); err != nil {
			it.valid = false
			return
		}
		it.value = make([]byte, valueLen)
		if _, err := it.blockReader.Read(it.value); err != nil {
			it.valid = false
			return
		}
		it.deleted = deletedByte == 1
		it.valid = true
		return // Successfully read one entry, stop here
	}
}

// Valid returns true if iterator is positioned at a valid entry
func (it *SSTableIterator) Valid() bool {
	return it.valid
}

// Key returns the current key
func (it *SSTableIterator) Key() []byte {
	return it.key
}

// Value returns the current value
func (it *SSTableIterator) Value() []byte {
	return it.value
}

// IsDeleted returns true if the current entry is a tombstone
func (it *SSTableIterator) IsDeleted() bool {
	return it.deleted
}

// FlushMemtableToSSTable writes a memtable to a new SSTable file
// Uses atomic rename for crash safety
// bitsPerKey controls bloom filter size (0 = no bloom filter)
func FlushMemtableToSSTable(mem *Memtable, path string, bitsPerKey int) error {
	// Write to temp file first
	tempPath := path + ".tmp"

	writer, err := NewSSTableWriter(tempPath, nil, bitsPerKey)
	if err != nil {
		return err
	}

	// Iterate through memtable (already sorted!)
	iter := mem.data.NewIterator()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		if err := writer.Add(iter.Key(), iter.Value(), iter.IsDeleted()); err != nil {
			writer.Close()
			os.Remove(tempPath) // Clean up temp file
			return err
		}
	}

	if err := writer.Finish(); err != nil {
		os.Remove(tempPath) // Clean up temp file
		return err
	}

	// Atomic rename: either succeeds completely or not at all
	// If crash happens here, temp file exists but final doesn't
	// On recovery, we can delete orphaned .tmp files
	return os.Rename(tempPath, path)
}
