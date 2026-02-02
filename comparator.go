package lsm

import "bytes"

type Comparator interface {
    // Compare returns -1, 0, or +1
    Compare(a, b []byte) int
    
    // Name returns comparator name (for SSTable compatibility)
    Name() string
}

// DefaultComparator compares keys as raw bytes
type DefaultComparator struct{}

func (DefaultComparator) Compare(a, b []byte) int {
    return bytes.Compare(a, b)
}

func (DefaultComparator) Name() string {
    return "lsm.DefaultComparator"
}

// add mvcc comparator later
