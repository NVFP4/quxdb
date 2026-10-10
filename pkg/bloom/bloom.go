package bloom

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

const (
	DefaultBitsPerKey = 10
	MaxBitsPerKey     = 32 // cap at the 32 bits/key optimum to bound hit/add CPU.

	slotBits = 64
	maxK     = 22      // k ~= MaxBitsPerKey * ln(2)
	maxSlots = 1 << 29 // max 4GiB bitset
)

var (
	ErrInvalidEncodedData       = errors.New("bloom: invalid encoded data")
	ErrInvalidFalsePositiveRate = errors.New("bloom: invalid false positive rate")
	ErrInvalidProbes            = errors.New("bloom: probes out of range [1, 22]")
	ErrInvalidBitsPerKey        = errors.New("bloom: bits per key out of range [1,32]")
	ErrFilterTooLarge           = errors.New("bloom: filter too large")
)

type Writer interface {
	Add(key []byte)
}

type BloomFilter struct {
	bits    []uint64
	mBits   uint64 // total bits
	kProbes uint8  // probes per key
}

func New(expectedKeys uint64) *BloomFilter {
	return NewWithBitsPerKey(expectedKeys, DefaultBitsPerKey)
}

func NewWithFalsePositiveRate(expectedKeys uint64, rate float64) (*BloomFilter, error) {
	if rate <= 0 || rate >= 1 || math.IsNaN(rate) {
		return nil, ErrInvalidFalsePositiveRate
	}

	bitsPerKey := math.Ceil(-math.Log(rate) / (math.Ln2 * math.Ln2))
	if bitsPerKey > MaxBitsPerKey || math.IsInf(bitsPerKey, 0) {
		return nil, ErrInvalidFalsePositiveRate
	}

	return NewWithBitsPerKey(expectedKeys, uint8(bitsPerKey)), nil
}

func NewWithBitsPerKey(expectedKeys uint64, bitsPerKey uint8) *BloomFilter {
	return NewWithProbes(expectedKeys, bitsPerKey, kForBitsPerKeyClamped(bitsPerKey))
}

func NewWithProbes(expectedKeys uint64, bitsPerKey uint8, kProbes uint8) *BloomFilter {
	if bitsPerKey == 0 || bitsPerKey > MaxBitsPerKey {
		panic(ErrInvalidBitsPerKey)
	}

	if kProbes == 0 || kProbes > maxK {
		panic(ErrInvalidProbes)
	}

	slots := slotsFor(expectedKeys, bitsPerKey)
	if slots > maxSlots {
		panic(ErrFilterTooLarge)
	}

	mBits := slots * slotBits
	return &BloomFilter{
		bits:    make([]uint64, int(slots)),
		mBits:   mBits,
		kProbes: kProbes,
	}
}

func (f *BloomFilter) Add(key []byte) {
	bits := f.bits
	mBits := f.mBits
	kProbes := f.kProbes

	h := xxhash.Sum64(key)
	base, delta := probeSeeds(h)

	for range kProbes {
		bit := fastReduce(base, mBits)
		setBit(bits, bit)
		base += delta
	}
}

func (f *BloomFilter) Contains(key []byte) bool {
	bits := f.bits
	mBits := f.mBits
	kProbes := f.kProbes

	h := xxhash.Sum64(key)
	base, delta := probeSeeds(h)

	for range kProbes {
		bit := fastReduce(base, mBits)
		if !hasBit(bits, bit) {
			return false
		}
		base += delta
	}

	return true
}

func (f *BloomFilter) Probes() int {
	return int(f.kProbes)
}

func (f *BloomFilter) BitLen() uint64 {
	return f.mBits
}

func (f *BloomFilter) SizeBytes() int {
	return len(f.bits) * 8
}

func (f *BloomFilter) MarshalLen() int {
	return 2*8 + f.SizeBytes()
}

func (f *BloomFilter) MarshalInto(dst []byte) (int, error) {
	off := 0

	binary.LittleEndian.PutUint64(dst, f.mBits)
	off += 8

	binary.LittleEndian.PutUint64(dst[off:], uint64(f.kProbes))
	off += 8

	for _, slot := range f.bits {
		binary.LittleEndian.PutUint64(dst[off:], slot)
		off += 8
	}

	return off, nil
}

func (f *BloomFilter) MarshalBinary() ([]byte, error) {
	out := make([]byte, f.MarshalLen())
	n, err := f.MarshalInto(out)
	if err != nil {
		return nil, err
	}
	return out[:n], nil
}

func (f *BloomFilter) UnmarshalSlice(src []byte) error {
	if len(src) < 16 {
		return fmt.Errorf("insufficient bytes to decode")
	}

	mBits := binary.LittleEndian.Uint64(src[0:8])
	kProbes := binary.LittleEndian.Uint64(src[8:16])

	bitBytes := src[16:]
	expectedSlots := len(bitBytes) / 8

	var bits []uint64
	if expectedSlots > 0 {
		// cast byte slices as a uint64 slice
		ptr := (*uint64)(unsafe.Pointer(&bitBytes[0]))
		bits = unsafe.Slice(ptr, expectedSlots)
	}

	f.bits = bits
	f.mBits = mBits
	f.kProbes = uint8(kProbes)

	return nil
}

func (f *BloomFilter) UnmarshalBinary(data []byte) error {
	if len(data) < 24 || len(data[16:])%8 != 0 {
		return ErrInvalidEncodedData
	}

	m := binary.LittleEndian.Uint64(data)
	k := binary.LittleEndian.Uint64(data[8:])
	payload := data[16:]
	slots := uint64(len(payload) / 8)
	if m == 0 || k == 0 || k > maxK || slots > maxSlots || slots*slotBits != m {
		return ErrInvalidEncodedData
	}

	bits := make([]uint64, int(slots))
	for i := range bits {
		bits[i] = binary.LittleEndian.Uint64(payload[i*8:])
	}

	f.bits = bits
	f.mBits = m
	f.kProbes = uint8(k)
	return nil
}

func setBit(bits []uint64, bit uint64) {
	slot := bitSlot(bits, bit)
	*slot |= uint64(1) << (bit & 63)
}

func hasBit(bits []uint64, bit uint64) bool {
	return *bitSlot(bits, bit)&(uint64(1)<<(bit&63)) != 0
}

func bitSlot(bits []uint64, bit uint64) *uint64 {
	return (*uint64)(unsafe.Add(unsafe.Pointer(unsafe.SliceData(bits)), uintptr(bit>>6)<<3))
}

func probeSeeds(hash uint64) (uint64, uint64) {
	// Kirsch-Mitzenmacher style double hashing gives k probe bits from one xxhash call.
	base := mix64(hash ^ 0x9e3779b97f4a7c15)
	delta := mix64(hash^0xd6e8feb86659fd93) | 1
	return base, delta
}

func slotsFor(expectedItems uint64, bitsPerKey uint8) uint64 {
	if expectedItems == 0 {
		expectedItems = 1
	}
	bpk64 := uint64(bitsPerKey)
	if expectedItems > (^uint64(0) / bpk64) {
		panic(ErrFilterTooLarge)
	}
	m := expectedItems * bpk64
	slots := (m-1)/slotBits + 1
	return slots
}

func kForBitsPerKeyClamped(bitsPerKey uint8) uint8 {
	if k := uint(bitsPerKey) * 69 / 100; k >= maxK {
		return maxK
	} else if k < 1 {
		return 1
	} else {
		return uint8(k)
	}
}

func fastReduce(hash uint64, n uint64) uint64 {
	// Multiply-high reduction maps uint64 to [0,n) without a hardware divide.
	hi, _ := bits.Mul64(hash, n)
	return hi
}

func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}
