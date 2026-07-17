package bloom

import (
	"crypto/rand"
	"errors"
	"fmt"
	"testing"
)

func TestBloomFilterContainsInsertedKeys(t *testing.T) {
	f := NewWithBitsPerKey(10_000, 10)
	keys := uniqueKeys(10_000, 16)

	for _, key := range keys {
		f.Add(key)
	}

	for i, key := range keys {
		if !f.Contains(key) {
			t.Fatalf("inserted key %d was not found", i)
		}
	}
}

func TestBloomFilterMarshalRoundTrip(t *testing.T) {
	f := NewWithBitsPerKey(1_000, 12)
	keys := uniqueKeys(1_000, 16)

	for _, key := range keys {
		f.Add(key)
	}

	data, err := f.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got BloomFilter
	if err := got.UnmarshalBinary(data); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got.Probes() != f.Probes() {
		t.Fatalf("probes = %d, want %d", got.Probes(), f.Probes())
	}
	if got.BitLen() != f.BitLen() {
		t.Fatalf("bits = %d, want %d", got.BitLen(), f.BitLen())
	}
	for i, key := range keys {
		if !got.Contains(key) {
			t.Fatalf("round-tripped filter missed inserted key %d", i)
		}
	}

	again, err := got.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal binary: %v", err)
	}
	if string(again) != string(data) {
		t.Fatal("round-tripped bytes changed")
	}
}

func TestBloomFilterNewWithProbes(t *testing.T) {
	f := NewWithProbes(1_000, 20, 5)
	keys := uniqueKeys(1_000, 16)

	if got := f.Probes(); got != 5 {
		t.Fatalf("probes = %d, want 5", got)
	}

	for _, key := range keys {
		f.Add(key)
	}
	for i, key := range keys {
		if !f.Contains(key) {
			t.Fatalf("inserted key %d was not found", i)
		}
	}

	data, err := f.MarshalBinary()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got BloomFilter
	if err := got.UnmarshalBinary(data); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got.Probes() != 5 {
		t.Fatalf("round-tripped probes = %d, want 5", got.Probes())
	}
}

func TestBloomFilterUnmarshalRejectsInvalidData(t *testing.T) {
	var f BloomFilter
	if err := f.UnmarshalBinary([]byte("short")); !errors.Is(err, ErrInvalidEncodedData) {
		t.Fatalf("short unmarshal error = %v, want %v", err, ErrInvalidEncodedData)
	}

	data, err := New(10).MarshalBinary()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if err := f.UnmarshalBinary(data[:len(data)-1]); !errors.Is(err, ErrInvalidEncodedData) {
		t.Fatalf("truncated data error = %v, want %v", err, ErrInvalidEncodedData)
	}
}

func BenchmarkBloomFilter(b *testing.B) {
	const keyCount = 1 << 16

	for _, keyLen := range []int{16, 32, 64, 128, 256} {
		b.Run(fmt.Sprintf("Key%d", keyLen), func(b *testing.B) {
			benchmarkFilter(b, keyCount, keyLen)
		})
	}
}

func benchmarkFilter(b *testing.B, keyCount int, keyLen int) {
	b.Run("Add", func(b *testing.B) {
		keys := uniqueKeys(keyCount, keyLen)
		f := NewWithBitsPerKey(uint64(keyCount), DefaultBitsPerKey)
		b.ReportMetric(float64(f.SizeBytes()), "filter_bytes")
		b.ReportAllocs()
		b.SetBytes(int64(keyLen))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			f.Add(keys[i&(keyCount-1)])
		}
	})

	b.Run("Contains/Hit", func(b *testing.B) {
		keys := uniqueKeys(keyCount, keyLen)
		f := NewWithBitsPerKey(uint64(keyCount), DefaultBitsPerKey)
		for _, key := range keys {
			f.Add(key)
		}

		b.ReportMetric(float64(f.SizeBytes()), "filter_bytes")
		b.ReportAllocs()
		b.SetBytes(int64(keyLen))
		b.ResetTimer()

		var hits int
		for i := 0; i < b.N; i++ {
			if f.Contains(keys[i&(keyCount-1)]) {
				hits++
			}
		}
		if hits != b.N {
			b.Fatalf("hits = %d, want %d", hits, b.N)
		}
	})

	b.Run("Contains/Miss", func(b *testing.B) {
		keys := uniqueKeys(keyCount, keyLen)
		misses := uniqueKeysExcluding(keyCount, keyLen, keys)
		f := NewWithBitsPerKey(uint64(keyCount), DefaultBitsPerKey)
		for _, key := range keys {
			f.Add(key)
		}

		b.ReportMetric(float64(f.SizeBytes()), "filter_bytes")
		b.ReportAllocs()
		b.SetBytes(int64(keyLen))
		b.ResetTimer()

		var hits int
		for i := 0; i < b.N; i++ {
			if f.Contains(misses[i&(keyCount-1)]) {
				hits++
			}
		}
		b.ReportMetric(float64(hits)/float64(b.N), "fp_rate")
	})
}

func uniqueKeys(n int, keyLen int) [][]byte {
	return uniqueKeysExcluding(n, keyLen, nil)
}

func uniqueKeysExcluding(n int, keyLen int, excludes [][]byte) [][]byte {
	seen := make(map[string]struct{}, n+len(excludes))
	for _, key := range excludes {
		seen[string(key)] = struct{}{}
	}

	keys := make([][]byte, n)
	for i := range keys {
		for {
			key := uniqueKey(keyLen)
			k := string(key)
			if _, ok := seen[k]; ok {
				continue
			}

			seen[k] = struct{}{}
			keys[i] = key
			break
		}
	}
	return keys
}

func uniqueKey(keyLen int) []byte {
	key := make([]byte, keyLen)
	if _, err := rand.Read(key); err != nil {
		panic(err)
	}
	return key
}
