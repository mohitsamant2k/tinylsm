package lsm

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// BloomFilter is a space-efficient probabilistic data structure
// that tests whether an element is a member of a set.
// False positives are possible, but false negatives are not.
type BloomFilter struct {
	bits     []byte // bit array
	numBits  uint64 // total number of bits
	numHash  uint32 // number of hash functions
	numItems uint64 // number of items added
}

// NewBloomFilter creates a bloom filter with specific bits per key.
func NewBloomFilter(expectedItems int, bitsPerKey int) *BloomFilter {
	if expectedItems <= 0 {
		expectedItems = 1
	}
	if bitsPerKey <= 0 {
		bitsPerKey = 10
	}

	numBits := uint64(expectedItems * bitsPerKey)
	if numBits < 64 {
		numBits = 64
	}

	// Round up to nearest byte
	numBytes := (numBits + 7) / 8
	numBits = numBytes * 8

	// Optimal hash functions for given bits per key: k = bitsPerKey * ln(2)
	numHash := uint32(float64(bitsPerKey) * math.Ln2)
	if numHash < 1 {
		numHash = 1
	}
	if numHash > 30 {
		numHash = 30
	}

	return &BloomFilter{
		bits:    make([]byte, numBytes),
		numBits: numBits,
		numHash: numHash,
	}
}

// Add inserts a key into the bloom filter
func (bf *BloomFilter) Add(key []byte) {
	h1, h2 := bf.hash(key)

	for i := uint32(0); i < bf.numHash; i++ {
		// Double hashing: hash_i = h1 + i * h2
		idx := (uint64(h1) + uint64(i)*uint64(h2)) % bf.numBits
		bf.setBit(idx)
	}

	bf.numItems++
}

// MayContain returns true if the key might be in the set.
// Returns false if the key is definitely NOT in the set.
func (bf *BloomFilter) MayContain(key []byte) bool {
	h1, h2 := bf.hash(key)

	for i := uint32(0); i < bf.numHash; i++ {
		idx := (uint64(h1) + uint64(i)*uint64(h2)) % bf.numBits
		if !bf.getBit(idx) {
			return false
		}
	}

	return true
}

// hash computes two 32-bit hash values for double hashing
func (bf *BloomFilter) hash(key []byte) (uint32, uint32) {
	// Use FNV-1a for first hash
	h1 := fnv.New32a()
	h1.Write(key)
	hash1 := h1.Sum32()

	// Use FNV-1 for second hash
	h2 := fnv.New32()
	h2.Write(key)
	hash2 := h2.Sum32()

	// Ensure hash2 is odd (for better distribution in double hashing)
	if hash2%2 == 0 {
		hash2++
	}

	return hash1, hash2
}

// setBit sets the bit at the given index to 1
func (bf *BloomFilter) setBit(idx uint64) {
	byteIdx := idx / 8
	bitIdx := idx % 8
	bf.bits[byteIdx] |= (1 << bitIdx)
}

// getBit returns true if the bit at the given index is 1
func (bf *BloomFilter) getBit(idx uint64) bool {
	byteIdx := idx / 8
	bitIdx := idx % 8
	return (bf.bits[byteIdx] & (1 << bitIdx)) != 0
}

// NumItems returns the number of items added to the filter
func (bf *BloomFilter) NumItems() uint64 {
	return bf.numItems
}

// Size returns the size of the bloom filter in bytes
func (bf *BloomFilter) Size() int {
	return len(bf.bits)
}

// FalsePositiveRate returns the estimated false positive rate
// based on the current number of items
func (bf *BloomFilter) FalsePositiveRate() float64 {
	if bf.numItems == 0 {
		return 0
	}
	// p = (1 - e^(-k*n/m))^k
	k := float64(bf.numHash)
	n := float64(bf.numItems)
	m := float64(bf.numBits)
	return math.Pow(1-math.Exp(-k*n/m), k)
}

// Encode serializes the bloom filter to bytes
func (bf *BloomFilter) Encode() []byte {
	// Format: [numBits:8][numHash:4][numItems:8][bits...]
	buf := make([]byte, 8+4+8+len(bf.bits))

	binary.LittleEndian.PutUint64(buf[0:8], bf.numBits)
	binary.LittleEndian.PutUint32(buf[8:12], bf.numHash)
	binary.LittleEndian.PutUint64(buf[12:20], bf.numItems)
	copy(buf[20:], bf.bits)

	return buf
}

// DecodeBloomFilter deserializes a bloom filter from bytes
func DecodeBloomFilter(data []byte) (*BloomFilter, error) {
	if len(data) < 20 {
		return nil, ErrCorruptedData
	}

	numBits := binary.LittleEndian.Uint64(data[0:8])
	numHash := binary.LittleEndian.Uint32(data[8:12])
	numItems := binary.LittleEndian.Uint64(data[12:20])

	expectedSize := int((numBits + 7) / 8)
	if len(data) < 20+expectedSize {
		return nil, ErrCorruptedData
	}

	bits := make([]byte, expectedSize)
	copy(bits, data[20:20+expectedSize])

	return &BloomFilter{
		bits:     bits,
		numBits:  numBits,
		numHash:  numHash,
		numItems: numItems,
	}, nil
}

// Reset clears the bloom filter
func (bf *BloomFilter) Reset() {
	for i := range bf.bits {
		bf.bits[i] = 0
	}
	bf.numItems = 0
}
