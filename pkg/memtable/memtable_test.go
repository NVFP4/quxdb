package memtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memtableTestImpl struct {
	name string
	new  func(...Option) Memtable
}

var memtableTestImpls = []memtableTestImpl{
	{"Skiplist", func(opts ...Option) Memtable { return newSkiplistMemtable(opts...) }},
	{"BTree", func(opts ...Option) Memtable { return newBTreeMemtable(opts...) }},
}

func testIteratorValue(version int) []byte {
	value := fmt.Appendf(nil, "version-%03d:", version)
	return append(value, bytes.Repeat([]byte{'x'}, version)...)
}

func collectIteratorKeys(t *testing.T, m Memtable, start, end []byte) []string {
	t.Helper()

	var keys []string
	it := m.Iterator(start, end)
	for {
		key, _, ok := it.Next()
		if !ok {
			break
		}
		keys = append(keys, string(key))
	}
	require.NoError(t, it.Err())
	return keys
}

func TestGetSet(t *testing.T) {
	for _, impl := range memtableTestImpls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.new()

			value, ok := m.Get([]byte("missing"))
			assert.False(t, ok)
			assert.Nil(t, value)

			key := []byte("key")
			input := []byte("value")
			require.NoError(t, m.Set(key, input))
			key[0] = 'x'
			input[0] = 'x'

			value, ok = m.Get([]byte("key"))
			require.True(t, ok)
			assert.Equal(t, []byte("value"), value)
			assert.Equal(t, 1, m.Len())

			require.NoError(t, m.Set([]byte("key"), []byte("updated")))
			value, ok = m.Get([]byte("key"))
			require.True(t, ok)
			assert.Equal(t, []byte("updated"), value)
			assert.Equal(t, 1, m.Len())

			require.NoError(t, m.Set(nil, nil))
			value, ok = m.Get([]byte{})
			require.True(t, ok)
			assert.Empty(t, value)
			assert.Equal(t, 2, m.Len())
		})
	}
}

func TestSeek(t *testing.T) {
	for _, impl := range memtableTestImpls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.new()
			key, value, ok := m.Seek(nil)
			assert.False(t, ok)
			assert.Nil(t, key)
			assert.Nil(t, value)

			for _, key := range []string{"a", "c", "e"} {
				require.NoError(t, m.Set([]byte(key), []byte("value-"+key)))
			}

			tests := []struct {
				name      string
				seek      []byte
				wantKey   []byte
				wantValue []byte
				found     bool
			}{
				{"Nil", nil, []byte("a"), []byte("value-a"), true},
				{"BeforeFirst", []byte("0"), []byte("a"), []byte("value-a"), true},
				{"Exact", []byte("c"), []byte("c"), []byte("value-c"), true},
				{"Between", []byte("b"), []byte("c"), []byte("value-c"), true},
				{"AfterLast", []byte("z"), nil, nil, false},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					key, value, ok := m.Seek(tt.seek)
					assert.Equal(t, tt.found, ok)
					assert.Equal(t, tt.wantKey, key)
					assert.Equal(t, tt.wantValue, value)
				})
			}
		})
	}
}

func TestCapacity(t *testing.T) {
	for _, impl := range memtableTestImpls {
		t.Run(impl.name, func(t *testing.T) {
			t.Run("Negative", func(t *testing.T) {
				assert.PanicsWithValue(t, "memtable: negative capacity", func() {
					impl.new(WithCapacityBytes(-1))
				})
			})

			t.Run("Zero", func(t *testing.T) {
				m := impl.new(WithCapacityBytes(0))
				require.NoError(t, m.Set(nil, nil))
				require.ErrorIs(t, m.Set([]byte("a"), nil), ErrMemtableFull)
				assert.Equal(t, 1, m.Len())
				assert.Zero(t, m.SizeBytes())
				_, ok := m.Get(nil)
				assert.True(t, ok)
			})

			t.Run("Exact", func(t *testing.T) {
				key := []byte("key")
				value := []byte("value")
				capacity := len(key) + len(value)
				m := impl.new(WithCapacityBytes(capacity))

				require.NoError(t, m.Set(key, value))
				require.ErrorIs(t, m.Set([]byte("x"), nil), ErrMemtableFull)
				assert.Equal(t, 1, m.Len())
				assert.Equal(t, capacity, m.SizeBytes())
				got, ok := m.Get(key)
				require.True(t, ok)
				assert.Equal(t, value, got)
			})

			t.Run("Overflow", func(t *testing.T) {
				key := []byte("key")
				value := []byte("value")
				m := impl.new(WithCapacityBytes(len(key) + len(value) - 1))

				require.ErrorIs(t, m.Set(key, value), ErrMemtableFull)
				assert.Zero(t, m.Len())
				assert.Zero(t, m.SizeBytes())
				_, ok := m.Get(key)
				assert.False(t, ok)
			})

			t.Run("UpdateOverflow", func(t *testing.T) {
				key := []byte("key")
				value := []byte("value")
				m := impl.new(WithCapacityBytes(len(key) + len(value)))
				require.NoError(t, m.Set(key, value))

				require.ErrorIs(t, m.Set(key, []byte("larger-value")), ErrMemtableFull)
				got, ok := m.Get(key)
				require.True(t, ok)
				assert.Equal(t, value, got)
				assert.Equal(t, 1, m.Len())
				assert.Equal(t, len(key)+len(value), m.SizeBytes())
			})

			t.Run("AfterClear", func(t *testing.T) {
				key := []byte("key")
				value := []byte("value")
				m := impl.new(WithCapacityBytes(len(key) + len(value)))
				require.NoError(t, m.Set(key, value))
				m.Clear()
				require.NoError(t, m.Set(key, value))
			})
		})
	}
}

func TestIterator(t *testing.T) {
	const count = 256

	for _, impl := range memtableTestImpls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.new()
			keys := make([][]byte, count)
			values := make([][]byte, count)
			for i := range count {
				keys[i] = fmt.Appendf(nil, "key-%03d", i)
				values[i] = fmt.Appendf(nil, "value-%03d", i)
			}
			for _, i := range rand.New(rand.NewSource(42)).Perm(count) {
				require.NoError(t, m.Set(keys[i], values[i]))
			}

			it := m.Iterator(nil, nil)
			for i := range count {
				key, value, ok := it.Next()
				require.True(t, ok, "iterator ended at item %d", i)
				assert.Equal(t, keys[i], key)
				assert.Equal(t, values[i], value)
			}
			_, _, ok := it.Next()
			assert.False(t, ok)
		})
	}
}

func TestIteratorRanges(t *testing.T) {
	tests := []struct {
		name       string
		start, end []byte
		want       []string
	}{
		{"Unbounded", nil, nil, []string{"a", "b", "c", "d"}},
		{"StartExact", []byte("b"), nil, []string{"b", "c", "d"}},
		{"StartBetween", []byte("bb"), nil, []string{"c", "d"}},
		{"EndExact", nil, []byte("c"), []string{"a", "b", "c"}},
		{"EndBetween", nil, []byte("bb"), []string{"a", "b"}},
		{"Closed", []byte("b"), []byte("c"), []string{"b", "c"}},
		{"Inverted", []byte("d"), []byte("b"), nil},
		{"AfterLast", []byte("z"), nil, nil},
	}

	for _, impl := range memtableTestImpls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.new()
			for _, key := range []string{"d", "b", "a", "c"} {
				require.NoError(t, m.Set([]byte(key), []byte("value-"+key)))
			}

			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					assert.Equal(t, tt.want, collectIteratorKeys(t, m, tt.start, tt.end))
				})
			}
		})
	}
}

func TestIteratorIndependence(t *testing.T) {
	for _, impl := range memtableTestImpls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.new()
			for _, key := range []string{"a", "b", "c"} {
				require.NoError(t, m.Set([]byte(key), []byte("value-"+key)))
			}

			first := m.Iterator(nil, nil)
			second := m.Iterator([]byte("b"), nil)

			key, _, ok := first.Next()
			require.True(t, ok)
			assert.Equal(t, []byte("a"), key)
			key, _, ok = second.Next()
			require.True(t, ok)
			assert.Equal(t, []byte("b"), key)
			key, _, ok = first.Next()
			require.True(t, ok)
			assert.Equal(t, []byte("b"), key)
			key, _, ok = second.Next()
			require.True(t, ok)
			assert.Equal(t, []byte("c"), key)
			key, _, ok = first.Next()
			require.True(t, ok)
			assert.Equal(t, []byte("c"), key)
		})
	}
}

func TestIteratorErrors(t *testing.T) {
	for _, impl := range memtableTestImpls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.new()
			require.NoError(t, m.Set([]byte("a"), []byte("value")))
			it := m.Iterator(nil, nil)

			assert.NoError(t, it.Err())
			_, _, ok := it.Next()
			require.True(t, ok)
			assert.NoError(t, it.Err())
			_, _, ok = it.Next()
			assert.False(t, ok)
			assert.NoError(t, it.Err())
		})
	}
}

func TestIteratorExhaustion(t *testing.T) {
	for _, impl := range memtableTestImpls {
		t.Run(impl.name, func(t *testing.T) {
			empty := impl.new().Iterator(nil, nil)
			for range 3 {
				key, value, ok := empty.Next()
				assert.False(t, ok)
				assert.Nil(t, key)
				assert.Nil(t, value)
			}

			m := impl.new()
			require.NoError(t, m.Set([]byte("a"), []byte("value")))
			it := m.Iterator(nil, nil)
			_, _, ok := it.Next()
			require.True(t, ok)
			for range 3 {
				key, value, ok := it.Next()
				assert.False(t, ok)
				assert.Nil(t, key)
				assert.Nil(t, value)
			}
		})
	}
}

func TestIteratorConcurrency(t *testing.T) {
	const (
		userCount      = 128
		versionCount   = 32
		writerCount    = 4
		readerCount    = 4
		scansPerReader = 8
	)

	allowedValues := make(map[string]struct{}, versionCount+1)
	for version := 0; version <= versionCount; version++ {
		allowedValues[string(testIteratorValue(version))] = struct{}{}
	}

	for _, impl := range memtableTestImpls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.new(
				WithCapacityBytes(32<<20),
				WithComparator(compareMVCCKey),
			)
			keys := make([][][]byte, userCount)
			planned := make(map[string]struct{}, userCount*(versionCount+1))
			for user := range userCount {
				userKey := fmt.Appendf(nil, "user-%03d", user)
				keys[user] = make([][]byte, versionCount+1)
				for version := 0; version <= versionCount; version++ {
					key := makeMVCCKey(userKey, uint64(version), mvccOpSet)
					keys[user][version] = key
					planned[string(key)] = struct{}{}
				}
				require.NoError(t, m.Set(keys[user][0], testIteratorValue(0)))
			}

			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(writerCount + readerCount)

			for writerID := range writerCount {
				go func() {
					defer wg.Done()
					<-start

					// Growing updates preserve old borrowed values while new keys reshape the index.
					for version := 1; version <= versionCount; version++ {
						value := testIteratorValue(version)
						for user := writerID; user < userCount; user += writerCount {
							if err := m.Set(keys[user][version], value); err != nil {
								t.Errorf("Set(%q): %v", keys[user][version], err)
								return
							}
							if err := m.Set(keys[user][0], value); err != nil {
								t.Errorf("Set(%q): %v", keys[user][0], err)
								return
							}
						}
						runtime.Gosched()
					}
				}()
			}

			for range readerCount {
				go func() {
					defer wg.Done()
					<-start

					for range scansPerReader {
						it := m.Iterator(nil, nil)
						var previous []byte
						count := 0
						for {
							key, value, ok := it.Next()
							if !ok {
								break
							}
							if previous != nil && compareMVCCKey(previous, key) >= 0 {
								t.Errorf("iterator out of order: %x then %x", previous, key)
								return
							}
							if _, ok := planned[string(key)]; !ok {
								t.Errorf("iterator returned unknown key %x", key)
								return
							}

							userLen := len(key) - mvccKeyTrailerLen
							version := binary.LittleEndian.Uint64(key[userLen : userLen+8])
							if version == 0 {
								if _, ok := allowedValues[string(value)]; !ok {
									t.Errorf("iterator key %x returned invalid value %q", key, value)
									return
								}
							} else if !bytes.Equal(value, testIteratorValue(int(version))) {
								t.Errorf("iterator key %x returned invalid value %q", key, value)
								return
							}

							previous = append(previous[:0], key...)
							count++
							if count > len(planned) {
								t.Errorf("iterator returned more than %d items", len(planned))
								return
							}
						}
						if err := it.Err(); err != nil {
							t.Errorf("iterator error: %v", err)
							return
						}
					}
				}()
			}

			close(start)
			wg.Wait()

			it := m.Iterator(nil, nil)
			for user := range userCount {
				for version := versionCount; version >= 0; version-- {
					key, value, ok := it.Next()
					require.True(t, ok)
					assert.Equal(t, keys[user][version], key)
					if version == 0 {
						assert.Equal(t, testIteratorValue(versionCount), value)
					} else {
						assert.Equal(t, testIteratorValue(version), value)
					}
				}
			}
			_, _, ok := it.Next()
			assert.False(t, ok)
			assert.NoError(t, it.Err())
			assert.Equal(t, len(planned), m.Len())
		})
	}
}

func TestBorrowedViews(t *testing.T) {
	for _, impl := range memtableTestImpls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.new()
			require.NoError(t, m.Set([]byte("a"), []byte("value-a")))
			require.NoError(t, m.Set([]byte("b"), []byte("value-b")))

			value, ok := m.Get([]byte("a"))
			require.True(t, ok)
			assert.Equal(t, len(value), cap(value))

			key, value, ok := m.Seek([]byte("a"))
			require.True(t, ok)
			assert.Equal(t, len(key), cap(key))
			assert.Equal(t, len(value), cap(value))

			key, value, ok = m.Iterator([]byte("a"), []byte("b")).Next()
			require.True(t, ok)
			assert.Equal(t, len(key), cap(key))
			assert.Equal(t, len(value), cap(value))
		})
	}
}

func TestSize(t *testing.T) {
	for _, impl := range memtableTestImpls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.new()
			assert.Zero(t, m.SizeBytes())

			require.NoError(t, m.Set([]byte("abc"), []byte("12")))
			assert.Equal(t, 5, m.SizeBytes())
			require.NoError(t, m.Set([]byte("abc"), []byte("12345")))
			assert.Equal(t, 8, m.SizeBytes())
			require.NoError(t, m.Set([]byte("abc"), []byte("x")))
			assert.Equal(t, 4, m.SizeBytes())
			require.NoError(t, m.Set([]byte("z"), []byte("vv")))
			assert.Equal(t, 7, m.SizeBytes())
			assert.Equal(t, 2, m.Len())
		})
	}
}

func TestClear(t *testing.T) {
	for _, impl := range memtableTestImpls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.new()
			for i := range 200 {
				require.NoError(t, m.Set(fmt.Appendf(nil, "key-%03d", i), []byte("value")))
			}

			m.Clear()
			assert.Zero(t, m.Len())
			assert.Zero(t, m.SizeBytes())
			_, ok := m.Get([]byte("key-000"))
			assert.False(t, ok)
			_, _, ok = m.Seek(nil)
			assert.False(t, ok)
			_, _, ok = m.Iterator(nil, nil).Next()
			assert.False(t, ok)

			m.Clear()
			require.NoError(t, m.Set([]byte("new"), []byte("value")))
			value, ok := m.Get([]byte("new"))
			require.True(t, ok)
			assert.Equal(t, []byte("value"), value)
			assert.Equal(t, 1, m.Len())
		})
	}
}

func TestClearComparator(t *testing.T) {
	for _, impl := range memtableTestImpls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.new(WithComparator(compareMVCCKey))
			require.NoError(t, m.Set(makeMVCCKey([]byte("key"), 1, 1), nil))
			m.Clear()

			newer := makeMVCCKey([]byte("key"), 2, 1)
			older := makeMVCCKey([]byte("key"), 1, 1)
			require.NoError(t, m.Set(older, nil))
			require.NoError(t, m.Set(newer, nil))
			key, _, ok := m.Iterator(nil, nil).Next()
			require.True(t, ok)
			assert.Equal(t, newer, key)
		})
	}
}

func TestComparator(t *testing.T) {
	for _, impl := range memtableTestImpls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.new(WithComparator(compareMVCCKey))
			keys := [][]byte{
				makeMVCCKey([]byte("a"), 3, 1),
				makeMVCCKey([]byte("a"), 2, 2),
				makeMVCCKey([]byte("a"), 2, 1),
				makeMVCCKey([]byte("a"), 1, 1),
				makeMVCCKey([]byte("b"), 4, 1),
			}
			for i, key := range keys {
				require.NoError(t, m.Set(key, fmt.Appendf(nil, "value-%d", i)))
			}

			assert.Equal(t, 5, m.Len())
			assert.Equal(t, []string{
				string(keys[0]),
				string(keys[1]),
				string(keys[2]),
				string(keys[3]),
				string(keys[4]),
			}, collectIteratorKeys(t, m, nil, nil))

			seek := makeMVCCKey([]byte("a"), ^uint64(0), 0xff)
			key, value, ok := m.Seek(seek)
			require.True(t, ok)
			assert.Equal(t, keys[0], key)
			assert.Equal(t, []byte("value-0"), value)

			require.NoError(t, m.Set(keys[1], []byte("updated")))
			value, ok = m.Get(keys[1])
			require.True(t, ok)
			assert.Equal(t, []byte("updated"), value)
			assert.Equal(t, 5, m.Len())
		})
	}
}

func TestOptions(t *testing.T) {
	for _, impl := range memtableTestImpls {
		t.Run(impl.name+"/NilComparator", func(t *testing.T) {
			assert.PanicsWithValue(t, "memtable: nil comparator", func() {
				impl.new(WithComparator(nil))
			})
		})
	}

	assert.PanicsWithValue(t, "invalid memtable type", func() {
		New(MemTableType(0))
	})
}

func TestConcurrency(t *testing.T) {
	const (
		writerCount     = 8
		readerCount     = 8
		writesPerWriter = 2_000
		readsPerReader  = 4_000
	)

	for _, impl := range memtableTestImpls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.new()
			totalWrites := writerCount * writesPerWriter
			writeKeys := make([][]byte, totalWrites)
			writeValues := make([][]byte, totalWrites)
			published := make([]atomic.Bool, totalWrites)
			expectedSize := 0
			for i := range totalWrites {
				writeKeys[i] = fmt.Appendf(nil, "write-%06d", i)
				writeValues[i] = fmt.Appendf(nil, "write-value-%06d", i)
				expectedSize += len(writeKeys[i]) + len(writeValues[i])
			}

			checkGet := func(key, want []byte) bool {
				got, ok := m.Get(key)
				if !ok || !bytes.Equal(got, want) {
					t.Errorf("Get(%q) = (%q, %t), want (%q, true)", key, got, ok, want)
					return false
				}
				return true
			}
			checkSeek := func(key, want []byte) bool {
				gotKey, gotValue, ok := m.Seek(key)
				if !ok || !bytes.Equal(gotKey, key) || !bytes.Equal(gotValue, want) {
					t.Errorf(
						"Seek(%q) = (%q, %q, %t), want (%q, %q, true)",
						key, gotKey, gotValue, ok, key, want,
					)
					return false
				}
				return true
			}

			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(writerCount + readerCount)

			for writerID := range writerCount {
				go func() {
					defer wg.Done()
					<-start

					first := writerID * writesPerWriter
					for i := range writesPerWriter {
						idx := first + i
						if err := m.Set(writeKeys[idx], writeValues[idx]); err != nil {
							t.Errorf("Set(%q): %v", writeKeys[idx], err)
							return
						}
						published[idx].Store(true)
						if i%64 == 0 {
							runtime.Gosched()
						}
					}
				}()
			}

			for readerID := range readerCount {
				go func() {
					defer wg.Done()
					<-start

					rng := rand.New(rand.NewSource(int64(readerID + 1)))
					for reads := range readsPerReader {
						writeIdx := rng.Intn(totalWrites)
						if published[writeIdx].Load() {
							if reads%2 == 0 {
								if !checkSeek(writeKeys[writeIdx], writeValues[writeIdx]) {
									return
								}
							} else if !checkGet(writeKeys[writeIdx], writeValues[writeIdx]) {
								return
							}
						}
						if reads%16 == 0 {
							_ = m.Len()
							_ = m.SizeBytes()
						}
					}
				}()
			}

			close(start)
			wg.Wait()

			assert.Equal(t, totalWrites, m.Len())
			assert.Equal(t, expectedSize, m.SizeBytes())
			for i := range totalWrites {
				value, ok := m.Get(writeKeys[i])
				require.True(t, ok, "missing key %q", writeKeys[i])
				assert.Equal(t, writeValues[i], value)
			}

			it := m.Iterator(nil, nil)
			for i := range totalWrites {
				key, value, ok := it.Next()
				require.True(t, ok, "iterator ended at item %d", i)
				assert.Equal(t, writeKeys[i], key)
				assert.Equal(t, writeValues[i], value)
			}
			_, _, ok := it.Next()
			assert.False(t, ok)
			assert.NoError(t, it.Err())
		})
	}
}

func TestConcurrencyOverwrite(t *testing.T) {
	const (
		keyCount        = 32
		writerCount     = 8
		readerCount     = 8
		writesPerWriter = 1_000
		readsPerReader  = 2_000
	)

	for _, impl := range memtableTestImpls {
		t.Run(impl.name, func(t *testing.T) {
			m := impl.new(WithCapacityBytes(32 << 20))
			keys := make([][]byte, keyCount)
			for i := range keyCount {
				keys[i] = fmt.Appendf(nil, "hot-%02d", i)
				require.NoError(t, m.Set(keys[i], []byte("value-00-00000000")))
			}

			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(writerCount + readerCount)

			for writerID := range writerCount {
				go func() {
					defer wg.Done()
					<-start

					for i := range writesPerWriter {
						key := keys[(writerID+i)%keyCount]
						value := fmt.Appendf(nil, "value-%02d-%08d", writerID, i)
						if err := m.Set(key, value); err != nil {
							t.Errorf("Set(%q): %v", key, err)
							return
						}
						if i%64 == 0 {
							runtime.Gosched()
						}
					}
				}()
			}

			for readerID := range readerCount {
				go func() {
					defer wg.Done()
					<-start

					rng := rand.New(rand.NewSource(int64(readerID + 1)))
					for range readsPerReader {
						key := keys[rng.Intn(keyCount)]
						value, ok := m.Get(key)
						if !ok || len(value) != len("value-00-00000000") {
							t.Errorf("Get(%q) returned found=%t len=%d", key, ok, len(value))
							return
						}
						foundKey, value, ok := m.Seek(key)
						if !ok || !bytes.Equal(foundKey, key) || len(value) != len("value-00-00000000") {
							t.Errorf("Seek(%q) returned key=%q found=%t len=%d", key, foundKey, ok, len(value))
							return
						}
					}
				}()
			}

			close(start)
			wg.Wait()

			assert.Equal(t, keyCount, m.Len())
			for _, key := range keys {
				value, ok := m.Get(key)
				require.True(t, ok)
				assert.Len(t, value, len("value-00-00000000"))
			}
		})
	}
}

const (
	mvccKeyTrailerLen       = 8 + 1
	mvccOpSet         uint8 = 1
	mvccOpSeekMax     uint8 = 0xff
)

func makeMVCCKey(userKey []byte, seq uint64, op uint8) []byte {
	key := make([]byte, len(userKey)+mvccKeyTrailerLen)
	copy(key, userKey)
	binary.LittleEndian.PutUint64(key[len(userKey):len(userKey)+8], seq)
	key[len(userKey)+8] = op
	return key
}

func compareMVCCKey(a, b []byte) int {
	aUserLen := len(a) - mvccKeyTrailerLen
	bUserLen := len(b) - mvccKeyTrailerLen
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
