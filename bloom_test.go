package lsm

import (
	"fmt"
	"testing"
)

func TestBloomFilterBasic(t *testing.T) {
	bf := NewBloomFilter(100, 10)

	// Add some keys
	keys := [][]byte{
		[]byte("hello"),
		[]byte("world"),
		[]byte("foo"),
		[]byte("bar"),
	}

	for _, key := range keys {
		bf.Add(key)
	}

	// All added keys should be found
	for _, key := range keys {
		if !bf.MayContain(key) {
			t.Errorf("Key %s should be found (false negative!)", key)
		}
	}

	// NumItems should match
	if bf.NumItems() != uint64(len(keys)) {
		t.Errorf("Expected %d items, got %d", len(keys), bf.NumItems())
	}
}

func TestBloomFilterNoFalseNegatives(t *testing.T) {
	bf := NewBloomFilter(1000, 10)

	// Add 1000 keys
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		bf.Add(key)
	}

	// All added keys MUST be found (no false negatives allowed)
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		if !bf.MayContain(key) {
			t.Fatalf("FALSE NEGATIVE: Key %s was added but not found!", key)
		}
	}
}

func TestBloomFilterFalsePositiveRate(t *testing.T) {
	expectedItems := 10000
	bitsPerKey := 10 // ~1% expected false positive rate

	bf := NewBloomFilter(expectedItems, bitsPerKey)

	// Add keys with prefix "added_"
	for i := 0; i < expectedItems; i++ {
		key := []byte(fmt.Sprintf("added_%d", i))
		bf.Add(key)
	}

	// Test keys that were NOT added (prefix "notadded_")
	falsePositives := 0
	testCount := 10000

	for i := 0; i < testCount; i++ {
		key := []byte(fmt.Sprintf("notadded_%d", i))
		if bf.MayContain(key) {
			falsePositives++
		}
	}

	// Calculate actual false positive rate
	actualFPRate := float64(falsePositives) / float64(testCount)
	expectedFPRate := 0.01 // ~1% for 10 bits per key

	t.Logf("False positives: %d / %d = %.2f%%", falsePositives, testCount, actualFPRate*100)
	t.Logf("Expected FP rate: ~%.2f%%", expectedFPRate*100)
	t.Logf("Estimated FP rate: %.2f%%", bf.FalsePositiveRate()*100)

	// Allow some margin (should be less than 3% for 10 bits/key)
	if actualFPRate > 0.03 {
		t.Errorf("False positive rate too high: %.2f%% (expected < 3%%)", actualFPRate*100)
	}
}

func TestBloomFilterEncodeDecode(t *testing.T) {
	bf := NewBloomFilter(100, 10)

	// Add some keys
	keys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
	}

	for _, key := range keys {
		bf.Add(key)
	}

	// Encode
	encoded := bf.Encode()

	// Decode
	decoded, err := DecodeBloomFilter(encoded)
	if err != nil {
		t.Fatalf("Failed to decode: %v", err)
	}

	// Verify all keys still found
	for _, key := range keys {
		if !decoded.MayContain(key) {
			t.Errorf("Key %s not found after decode", key)
		}
	}

	// Verify metadata
	if decoded.NumItems() != bf.NumItems() {
		t.Errorf("NumItems mismatch: expected %d, got %d", bf.NumItems(), decoded.NumItems())
	}

	if decoded.Size() != bf.Size() {
		t.Errorf("Size mismatch: expected %d, got %d", bf.Size(), decoded.Size())
	}
}

func TestBloomFilterDecodeErrors(t *testing.T) {
	// Too short data
	_, err := DecodeBloomFilter([]byte{1, 2, 3})
	if err != ErrCorruptedData {
		t.Errorf("Expected ErrCorruptedData for short data, got %v", err)
	}

	// Data too short for declared size
	badData := make([]byte, 20) // Header only, no bits
	_, err = DecodeBloomFilter(badData)
	// numBits will be 0, expectedSize will be 0, so this should pass
	// Let's create a case where numBits > 0 but bits are missing
	badData2 := make([]byte, 25)
	badData2[0] = 64 // numBits = 64 (requires 8 bytes)
	_, err = DecodeBloomFilter(badData2)
	if err != ErrCorruptedData {
		t.Errorf("Expected ErrCorruptedData for truncated data, got %v", err)
	}
}

func TestBloomFilterEmpty(t *testing.T) {
	bf := NewBloomFilter(100, 10)

	// Empty filter should not contain anything
	if bf.MayContain([]byte("anything")) {
		t.Error("Empty bloom filter should not contain any keys")
	}

	if bf.NumItems() != 0 {
		t.Errorf("Expected 0 items, got %d", bf.NumItems())
	}

	if bf.FalsePositiveRate() != 0 {
		t.Errorf("Expected 0 FP rate for empty filter, got %f", bf.FalsePositiveRate())
	}
}

func TestBloomFilterReset(t *testing.T) {
	bf := NewBloomFilter(100, 10)

	// Add keys
	bf.Add([]byte("key1"))
	bf.Add([]byte("key2"))

	if bf.NumItems() != 2 {
		t.Errorf("Expected 2 items, got %d", bf.NumItems())
	}

	// Reset
	bf.Reset()

	// Should be empty
	if bf.NumItems() != 0 {
		t.Errorf("Expected 0 items after reset, got %d", bf.NumItems())
	}

	if bf.MayContain([]byte("key1")) {
		t.Error("Key1 should not be found after reset")
	}
}

func TestBloomFilterSize(t *testing.T) {
	testCases := []struct {
		expectedItems int
		bitsPerKey    int
		minBytes      int
		maxBytes      int
	}{
		{100, 10, 125, 130},    // 100 * 10 = 1000 bits = 125 bytes
		{1000, 10, 1250, 1260}, // 1000 * 10 = 10000 bits = 1250 bytes
		{100, 5, 62, 70},       // 100 * 5 = 500 bits = ~63 bytes
	}

	for _, tc := range testCases {
		bf := NewBloomFilter(tc.expectedItems, tc.bitsPerKey)
		size := bf.Size()

		if size < tc.minBytes || size > tc.maxBytes {
			t.Errorf("NewBloomFilter(%d, %d): size %d not in range [%d, %d]",
				tc.expectedItems, tc.bitsPerKey, size, tc.minBytes, tc.maxBytes)
		}
	}
}

func TestBloomFilterDefaultValues(t *testing.T) {
	// Test with zero/negative values - should use defaults
	bf := NewBloomFilter(0, 0)

	if bf.Size() < 8 { // Minimum 64 bits = 8 bytes
		t.Errorf("Expected minimum size of 8 bytes, got %d", bf.Size())
	}

	// Should still work
	bf.Add([]byte("test"))
	if !bf.MayContain([]byte("test")) {
		t.Error("Key should be found")
	}
}

func TestBloomFilterManyKeys(t *testing.T) {
	bf := NewBloomFilter(10000, 10)

	// Add 10000 keys
	for i := 0; i < 10000; i++ {
		bf.Add([]byte(fmt.Sprintf("key%d", i)))
	}

	// Verify all keys present
	for i := 0; i < 10000; i++ {
		if !bf.MayContain([]byte(fmt.Sprintf("key%d", i))) {
			t.Fatalf("Key key%d not found", i)
		}
	}

	t.Logf("Successfully stored and retrieved 10000 keys")
	t.Logf("Filter size: %d bytes", bf.Size())
	t.Logf("Estimated FP rate: %.4f%%", bf.FalsePositiveRate()*100)
}

// Benchmark tests
func BenchmarkBloomFilterAdd(b *testing.B) {
	bf := NewBloomFilter(b.N, 10)
	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("key_%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Add(keys[i])
	}
}

func BenchmarkBloomFilterMayContain(b *testing.B) {
	bf := NewBloomFilter(10000, 10)

	// Pre-populate
	for i := 0; i < 10000; i++ {
		bf.Add([]byte(fmt.Sprintf("key_%d", i)))
	}

	keys := make([][]byte, b.N)
	for i := 0; i < b.N; i++ {
		keys[i] = []byte(fmt.Sprintf("key_%d", i%10000))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.MayContain(keys[i])
	}
}

func BenchmarkBloomFilterEncode(b *testing.B) {
	bf := NewBloomFilter(10000, 10)
	for i := 0; i < 10000; i++ {
		bf.Add([]byte(fmt.Sprintf("key_%d", i)))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Encode()
	}
}
