package memtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMemtable(t *testing.T) {
	impls := []struct {
		name    string
		Factory func(...Option) Memtable
	}{
		{"Skiplist", func(opts ...Option) Memtable { return newSkiplistMemtable(opts...) }},
		{"BTree", func(opts ...Option) Memtable { return newBTreeMemtable(opts...) }},
	}

	for _, impl := range impls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.Factory()

			// 1. Set / Get
			key := []byte("foo")
			val := []byte("bar")
			m.Set(key, val)

			got, ok := m.Get(key)
			assert.True(t, ok, "Get failed: key not found")
			assert.Equal(t, val, got, "Get returned wrong value")

			// 2. Overwrite
			newVal := []byte("baz")
			m.Set(key, newVal)

			got, ok = m.Get(key)
			assert.True(t, ok, "Get failed after overwrite: key not found")
			assert.Equal(t, newVal, got, "Get returned wrong value after overwrite")

			// 4. Delete
			m.Delete(key)

			_, ok = m.Get(key)
			assert.False(t, ok, "Get returned ok=true after delete")

			// 5. Len
			m2 := impl.Factory()
			assert.Equal(t, 0, m2.Len(), "Len not 0 for new table")

			for i := range 100 {
				m2.Set(fmt.Appendf(nil, "key-%03d", i), []byte("val"))
			}
			assert.Equal(t, 100, m2.Len(), "Len expected 100")

			// 6. Iterators
			t.Run("Iterators", func(t *testing.T) {
				// All
				allCount := 0
				for range m2.Iter() {
					allCount++
				}
				assert.Equal(t, 100, allCount, "All iterator expected 100 items")
			})
		})
	}
}

func TestMemtableIter(t *testing.T) {
	impls := []struct {
		name    string
		Factory func(...Option) Memtable
	}{
		{"Skiplist", func(opts ...Option) Memtable { return newSkiplistMemtable(opts...) }},
		{"BTree", func(opts ...Option) Memtable { return newBTreeMemtable(opts...) }},
	}

	for _, impl := range impls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.Factory()

			// 1. Manual keys in random order
			keys := []string{"banana", "apple", "cherry", "date", "apricot", "berry", "applepie", "a", "b", "c"}
			for _, k := range keys {
				m.Set([]byte(k), []byte("val"))
			}

			var iterated []string
			for k := range m.Iter() {
				iterated = append(iterated, string(k))
			}

			expected := slices.Clone(keys)
			slices.Sort(expected)
			assert.Equal(t, expected, iterated, "Keys are not sorted")

			seekKey, seekVal, ok := m.SeekGE([]byte("applep"))
			assert.True(t, ok, "SeekGE should find first key >= seek key")
			assert.Equal(t, []byte("applepie"), seekKey)
			assert.Equal(t, []byte("val"), seekVal)

			var fromApple []string
			for k := range m.IterFrom([]byte("apple")) {
				fromApple = append(fromApple, string(k))
			}
			assert.Equal(t, []string{"apple", "applepie", "apricot", "b", "banana", "berry", "c", "cherry", "date"}, fromApple, "IterFrom should include first key >= start")

			var fromBetweenKeys []string
			for k := range m.IterFrom([]byte("bb")) {
				fromBetweenKeys = append(fromBetweenKeys, string(k))
			}
			assert.Equal(t, []string{"berry", "c", "cherry", "date"}, fromBetweenKeys, "IterFrom should start at next key when start is absent")

			var ranged []string
			for k := range m.IterRange([]byte("apple"), []byte("c")) {
				ranged = append(ranged, string(k))
			}
			assert.Equal(t, []string{"apple", "applepie", "apricot", "b", "banana", "berry", "c"}, ranged, "IterRange should include both bounds")

			var emptyRange []string
			for k := range m.IterRange([]byte("d"), []byte("a")) {
				emptyRange = append(emptyRange, string(k))
			}
			assert.Empty(t, emptyRange, "IterRange should be empty when start is after end")

			// 2. Larger randomized test
			m2 := impl.Factory()
			r := rand.New(rand.NewSource(42))
			count := 2000
			uniqueKeys := make(map[string]struct{})
			for range count {
				keyLen := r.Intn(32) + 1
				key := make([]byte, keyLen)
				r.Read(key)
				m2.Set(key, []byte("val"))
				uniqueKeys[string(key)] = struct{}{}
			}

			var lastKey []byte
			iterCount := 0
			for k := range m2.Iter() {
				if lastKey != nil {
					assert.Greater(t, bytes.Compare(k, lastKey), 0, "Iterator out of order at position %d: %x -> %x", iterCount, lastKey, k)
				}
				lastKey = bytes.Clone(k)
				iterCount++
			}

			assert.Equal(t, len(uniqueKeys), iterCount, "Iterator returned wrong number of unique keys")
		})
	}
}

func TestMemtableComparator(t *testing.T) {
	impls := []struct {
		name    string
		Factory func(...Option) Memtable
	}{
		{"Skiplist", func(opts ...Option) Memtable { return newSkiplistMemtable(opts...) }},
		{"BTree", func(opts ...Option) Memtable { return newBTreeMemtable(opts...) }},
	}

	for _, impl := range impls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.Factory(WithComparator(func(a, b []byte) int {
				if c := bytes.Compare(a[:1], b[:1]); c != 0 {
					return c
				}
				return int(b[1]) - int(a[1])
			}))

			m.Set([]byte{'a', 3}, []byte("a3"))
			m.Set([]byte{'a', 1}, []byte("a1"))
			m.Set([]byte{'b', 2}, []byte("b2"))
			m.Set([]byte{'b', 1}, []byte("b1"))

			var iterated [][]byte
			for k := range m.Iter() {
				iterated = append(iterated, bytes.Clone(k))
			}
			assert.Equal(t, [][]byte{
				{'a', 3},
				{'a', 1},
				{'b', 2},
				{'b', 1},
			}, iterated)

			key, val, ok := m.SeekGE([]byte{'a', 2})
			assert.True(t, ok)
			assert.Equal(t, []byte{'a', 1}, key)
			assert.Equal(t, []byte("a1"), val)

			key, val, ok = m.SeekGE([]byte{'b', 3})
			assert.True(t, ok)
			assert.Equal(t, []byte{'b', 2}, key)
			assert.Equal(t, []byte("b2"), val)

			key, _, ok = m.SeekGE([]byte{'a', 0})
			assert.True(t, ok)
			assert.Equal(t, []byte{'b', 2}, key)
		})
	}
}

func TestMemtableConcurrency(t *testing.T) {
	impls := []struct {
		name    string
		Factory func(...Option) Memtable
	}{
		{"Skiplist", func(opts ...Option) Memtable { return newSkiplistMemtable(opts...) }},
		{"BTree", func(opts ...Option) Memtable { return newBTreeMemtable(opts...) }},
	}

	for _, impl := range impls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.Factory()
			const numGoroutines = 50
			const insertsPerGoroutine = 1000

			var wg sync.WaitGroup
			wg.Add(numGoroutines)

			for g := range numGoroutines {
				go func(gid int) {
					defer wg.Done()
					rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(gid)))
					for i := range insertsPerGoroutine {
						// Use keys that naturally hit different shards and some same shards
						// Key format: "key-<shard-byte>-<random>"
						key := fmt.Appendf(nil, "key-%d-%d", rng.Intn(256), i)
						m.Set(key, []byte("value"))

						// Occasionally read back
						if i%10 == 0 {
							_, _ = m.Get(key)
						}
					}
				}(g)
			}

			wg.Wait()

			total := m.Len()
			assert.Greater(t, total, 0, "Len is 0 after concurrent inserts")
		})
	}
}

const (
	benchUserKeyLen                  = 16
	benchInternalKeyTrailerLen       = 8 + 1
	benchOpSet                 uint8 = 1
	benchOpSeekMax             uint8 = 0xff
	benchIterKeys                    = 10_000
)

func makeInternalBenchKey(userKey []byte, seq uint64, op uint8) []byte {
	key := make([]byte, len(userKey)+benchInternalKeyTrailerLen)
	copy(key, userKey)
	binary.LittleEndian.PutUint64(key[len(userKey):len(userKey)+8], seq)
	key[len(userKey)+8] = op
	return key
}

func compareInternalBenchKey(a, b []byte) int {
	aUserLen := len(a) - benchInternalKeyTrailerLen
	bUserLen := len(b) - benchInternalKeyTrailerLen

	if c := bytes.Compare(a[:aUserLen], b[:bUserLen]); c != 0 {
		return c
	}

	aSeq := binary.LittleEndian.Uint64(a[aUserLen : aUserLen+8])
	bSeq := binary.LittleEndian.Uint64(b[bUserLen : bUserLen+8])
	if aSeq > bSeq {
		return -1
	}
	if aSeq < bSeq {
		return 1
	}

	aOp := a[aUserLen+8]
	bOp := b[bUserLen+8]
	if aOp > bOp {
		return -1
	}
	if aOp < bOp {
		return 1
	}
	return 0
}

func BenchmarkMemtable(b *testing.B) {
	impls := []struct {
		name    string
		Factory func(...Option) Memtable
	}{
		{"Skiplist", func(opts ...Option) Memtable { return newSkiplistMemtable(opts...) }},
		{"BTree", func(opts ...Option) Memtable { return newBTreeMemtable(opts...) }},
	}

	val := []byte("value")

	for _, impl := range impls {
		b.Run(impl.name, func(b *testing.B) {
			b.Run("Set/Unique/Seq", func(b *testing.B) {
				m := impl.Factory()
				keys := make([][]byte, b.N)
				for i := range b.N {
					keys[i] = fmt.Appendf(nil, "unique-seq:%016d", i)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					m.Set(keys[i], val)
				}
			})

			b.Run("Set/Unique/Rand", func(b *testing.B) {
				m := impl.Factory()
				rngLocal := rand.New(rand.NewSource(42))
				keys := make([][]byte, b.N)
				for i := range b.N {
					keys[i] = fmt.Appendf(nil, "unique-rand:%016x:%016d", rngLocal.Uint64(), i)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					m.Set(keys[i], val)
				}
			})

			b.Run("Get/Seq", func(b *testing.B) {
				keys := make([][]byte, b.N)
				for i := range b.N {
					keys[i] = fmt.Appendf(nil, "unique-seq:%016d", i)
				}

				m := impl.Factory()
				for i := 0; i < b.N; i++ {
					m.Set(keys[i], val)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					m.Get(keys[i])
				}
			})

			b.Run("Get/Rand", func(b *testing.B) {
				rngLocal := rand.New(rand.NewSource(42))
				keys := make([][]byte, b.N)
				for i := range b.N {
					keys[i] = fmt.Appendf(nil, "unique-rand:%016x:%016d", rngLocal.Uint64(), i)
				}

				indices := make([]int, b.N)
				for i := 0; i < b.N; i++ {
					indices[i] = rngLocal.Intn(b.N)
				}

				m := impl.Factory()
				for i := 0; i < b.N; i++ {
					m.Set(keys[i], val)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					m.Get(keys[indices[i]])
				}
			})

			b.Run("SeekGE/Seq", func(b *testing.B) {
				keys := make([][]byte, b.N)
				for i := range b.N {
					keys[i] = fmt.Appendf(nil, "unique-seq:%016d", i)
				}

				m := impl.Factory()
				for i := 0; i < b.N; i++ {
					m.Set(keys[i], val)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					m.SeekGE(keys[i])
				}
			})

			b.Run("SeekGE/Rand", func(b *testing.B) {
				rngLocal := rand.New(rand.NewSource(42))
				keys := make([][]byte, b.N)
				for i := range b.N {
					keys[i] = fmt.Appendf(nil, "unique-rand:%016x:%016d", rngLocal.Uint64(), i)
				}

				indices := make([]int, b.N)
				for i := 0; i < b.N; i++ {
					indices[i] = rngLocal.Intn(b.N)
				}

				m := impl.Factory()
				for i := 0; i < b.N; i++ {
					m.Set(keys[i], val)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					m.SeekGE(keys[indices[i]])
				}
			})

			b.Run("Iter/All", func(b *testing.B) {
				m := impl.Factory()
				for i := range benchIterKeys {
					m.Set(fmt.Appendf(nil, "unique-seq:%016d", i), val)
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					count := 0
					for range m.Iter() {
						count++
					}
					if count != benchIterKeys {
						b.Fatalf("expected %d, got %d", benchIterKeys, count)
					}
				}
			})

			b.Run("CustomKey", func(b *testing.B) {
				b.Run("Set/Unique/Seq", func(b *testing.B) {
					m := impl.Factory(WithComparator(compareInternalBenchKey))
					userKey := []byte("bench-user-key-0")
					keys := make([][]byte, b.N)
					for i := range b.N {
						keys[i] = makeInternalBenchKey(userKey, uint64(i)+1, benchOpSet)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						m.Set(keys[i], val)
					}
				})

				b.Run("Set/Unique/Rand", func(b *testing.B) {
					m := impl.Factory(WithComparator(compareInternalBenchKey))
					rngLocal := rand.New(rand.NewSource(42))
					keys := make([][]byte, b.N)
					for i := range b.N {
						userKey := make([]byte, benchUserKeyLen)
						binary.BigEndian.PutUint64(userKey[:8], rngLocal.Uint64())
						binary.BigEndian.PutUint64(userKey[8:16], uint64(i))
						keys[i] = makeInternalBenchKey(userKey, uint64(i)+1, benchOpSet)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						m.Set(keys[i], val)
					}
				})

				b.Run("Get/Seq", func(b *testing.B) {
					m := impl.Factory(WithComparator(compareInternalBenchKey))
					keys := make([][]byte, b.N)
					for i := range b.N {
						keys[i] = makeInternalBenchKey(fmt.Appendf(nil, "unique-seq:%016d", i), uint64(i)+1, benchOpSet)
					}

					for i := 0; i < b.N; i++ {
						m.Set(keys[i], val)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						m.Get(keys[i])
					}
				})

				b.Run("Get/Rand", func(b *testing.B) {
					m := impl.Factory(WithComparator(compareInternalBenchKey))
					rngLocal := rand.New(rand.NewSource(42))
					keys := make([][]byte, b.N)
					for i := range b.N {
						userKey := make([]byte, benchUserKeyLen)
						binary.BigEndian.PutUint64(userKey[:8], rngLocal.Uint64())
						binary.BigEndian.PutUint64(userKey[8:16], uint64(i))
						keys[i] = makeInternalBenchKey(userKey, uint64(i)+1, benchOpSet)
					}

					for i := 0; i < b.N; i++ {
						m.Set(keys[i], val)
					}

					indices := make([]int, b.N)
					for i := 0; i < b.N; i++ {
						indices[i] = rngLocal.Intn(b.N)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						m.Get(keys[indices[i]])
					}
				})

				b.Run("SeekGE/Seq", func(b *testing.B) {
					m := impl.Factory(WithComparator(compareInternalBenchKey))
					keys := make([][]byte, b.N)
					seekKeys := make([][]byte, b.N)
					for i := range b.N {
						userKey := fmt.Appendf(nil, "unique-seq:%016d", i)
						keys[i] = makeInternalBenchKey(userKey, uint64(i)+1, benchOpSet)
						seekKeys[i] = makeInternalBenchKey(userKey, uint64(b.N)+1, benchOpSeekMax)
					}

					for i := 0; i < b.N; i++ {
						m.Set(keys[i], val)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						m.SeekGE(seekKeys[i])
					}
				})

				b.Run("SeekGE/Rand", func(b *testing.B) {
					m := impl.Factory(WithComparator(compareInternalBenchKey))
					rngLocal := rand.New(rand.NewSource(42))
					keys := make([][]byte, b.N)
					seekKeys := make([][]byte, b.N)
					for i := range b.N {
						userKey := make([]byte, benchUserKeyLen)
						binary.BigEndian.PutUint64(userKey[:8], rngLocal.Uint64())
						binary.BigEndian.PutUint64(userKey[8:16], uint64(i))
						keys[i] = makeInternalBenchKey(userKey, uint64(i)+1, benchOpSet)
						seekKeys[i] = makeInternalBenchKey(userKey, uint64(b.N)+1, benchOpSeekMax)
					}
					for i := 0; i < b.N; i++ {
						m.Set(keys[i], val)
					}

					indices := make([]int, b.N)
					for i := 0; i < b.N; i++ {
						indices[i] = rngLocal.Intn(b.N)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						m.SeekGE(seekKeys[indices[i]])
					}
				})

				b.Run("Iter/All", func(b *testing.B) {
					m := impl.Factory(WithComparator(compareInternalBenchKey))
					for i := range benchIterKeys {
						userKey := fmt.Appendf(nil, "unique-seq:%016d", i)
						m.Set(makeInternalBenchKey(userKey, uint64(i)+1, benchOpSet), val)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						count := 0
						for range m.Iter() {
							count++
						}
						if count != benchIterKeys {
							b.Fatalf("expected %d, got %d", benchIterKeys, count)
						}
					}
				})
			})
		})
	}
}
