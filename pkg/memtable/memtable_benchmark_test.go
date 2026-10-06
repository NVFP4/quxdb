package memtable

import (
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	// room for the largest key set plus index overhead
	benchMemtableCapacity = 32 << 20
	benchRandSeed         = 7129
	benchKeyLen           = 16
	benchValueLen         = 128
	benchScanLen          = 100
	benchZipfTraceSize    = 1 << 10
)

var (
	benchSizes     = []int{1_000, 10_000, 100_000}
	benchReaders   = []int{2, 4, 8, 16}
	benchZipfSkews = []float64{1.01, 1.2, 1.5}
)

var benchImpls = []struct {
	name    string
	Factory func(...Option) Memtable
}{
	{"Skiplist", func(opts ...Option) Memtable {
		opts = append([]Option{WithCapacityBytes(benchMemtableCapacity)}, opts...)
		return newSkiplistMemtable(opts...)
	}},
	{"BTree", func(opts ...Option) Memtable {
		opts = append([]Option{WithCapacityBytes(benchMemtableCapacity)}, opts...)
		return newBTreeMemtable(opts...)
	}},
}

func BenchmarkMemtable(b *testing.B) {
	val := benchValue(benchValueLen)

	for _, impl := range benchImpls {
		b.Run("impl="+impl.name, func(b *testing.B) {
			for _, benchSize := range benchSizes {
				b.Run(fmt.Sprintf("keys=%d", benchSize), func(b *testing.B) {
					keys := benchKeys(b, benchSize)
					perm := rand.New(rand.NewSource(benchRandSeed)).Perm(benchSize)

					b.Run("Set/Seq", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							b.StopTimer()
							runtime.GC()
							m := impl.Factory()
							b.StartTimer()

							for i := range keys {
								if err := m.Set(keys[i], val); err != nil {
									b.Fatal(err)
								}
							}
						}
						reportPerKey(b, benchSize)
					})

					b.Run("Set/Rand", func(b *testing.B) {
						b.ReportAllocs()
						for b.Loop() {
							b.StopTimer()
							runtime.GC()
							m := impl.Factory()
							b.StartTimer()

							for i := range perm {
								if err := m.Set(keys[perm[i]], val); err != nil {
									b.Fatal(err)
								}
							}
						}
						reportPerKey(b, benchSize)
					})

					b.Run("Get/Seq", func(b *testing.B) {
						m := impl.Factory()
						benchFill(b, m, keys, val)

						b.ReportAllocs()
						i := 0
						for b.Loop() {
							m.Get(keys[i])
							i++
							if i == benchSize {
								i = 0
							}
						}
					})

					b.Run("Get/Rand", func(b *testing.B) {
						m := impl.Factory()
						benchFill(b, m, keys, val)

						b.ReportAllocs()
						i := 0
						for b.Loop() {
							m.Get(keys[perm[i]])
							i++
							if i == benchSize {
								i = 0
							}
						}
					})

					b.Run("Get/Missing", func(b *testing.B) {
						present := make([][]byte, benchSize)
						absent := make([][]byte, benchSize)
						for i := range benchSize {
							present[i] = benchKey(2 * i)
							absent[i] = benchKey(2*i + 1)
						}

						m := impl.Factory()
						benchFill(b, m, present, val)

						b.ReportAllocs()
						i := 0
						for b.Loop() {
							m.Get(absent[i])
							i++
							if i == benchSize {
								i = 0
							}
						}
					})

					b.Run("Get/Skewed", func(b *testing.B) {
						m := impl.Factory()
						benchFill(b, m, keys, val)

						for _, skew := range benchZipfSkews {
							b.Run(fmt.Sprintf("s=%.2f", skew), func(b *testing.B) {
								trace := makeBenchZipfTrace(benchSize, skew)
								b.ReportAllocs()
								i := 0
								for b.Loop() {
									m.Get(keys[trace[i]])
									i = (i + 1) & (len(trace) - 1)
								}
							})
						}
					})

					b.Run("Get/Concurrent", func(b *testing.B) {
						m := impl.Factory()
						benchFill(b, m, keys, val)

						for _, readers := range benchReaders {
							b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
								if readers > runtime.NumCPU() {
									b.Skipf("needs %d CPUs, have %d", readers, runtime.NumCPU())
								}
								oldProcs := runtime.GOMAXPROCS(readers)
								b.Cleanup(func() {
									runtime.GOMAXPROCS(oldProcs)
								})

								var workerID atomic.Uint64
								b.SetParallelism(1)
								b.ReportAllocs()
								b.ResetTimer()
								b.RunParallel(func(pb *testing.PB) {
									id := int(workerID.Add(1) - 1)
									i := id * (benchSize / readers)
									for pb.Next() {
										m.Get(keys[perm[i]])
										i++
										if i == benchSize {
											i = 0
										}
									}
								})
							})
						}
					})

					b.Run("Get/ConcurrentR+W", func(b *testing.B) {
						benchConcurrentRW(b, impl.Factory, keys, val, func(m Memtable, i int) {
							m.Get(keys[perm[i]])
						})
					})

					b.Run("Seek/Seq", func(b *testing.B) {
						m := impl.Factory()
						benchFill(b, m, keys, val)

						b.ReportAllocs()
						i := 0
						for b.Loop() {
							m.Seek(keys[i])
							i++
							if i == benchSize {
								i = 0
							}
						}
					})

					b.Run("Seek/Rand", func(b *testing.B) {
						m := impl.Factory()
						benchFill(b, m, keys, val)

						b.ReportAllocs()
						i := 0
						for b.Loop() {
							m.Seek(keys[perm[i]])
							i++
							if i == benchSize {
								i = 0
							}
						}
					})

					b.Run("Seek/Concurrent", func(b *testing.B) {
						m := impl.Factory()
						benchFill(b, m, keys, val)

						for _, readers := range benchReaders {
							b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
								if readers > runtime.NumCPU() {
									b.Skipf("needs %d CPUs, have %d", readers, runtime.NumCPU())
								}
								oldProcs := runtime.GOMAXPROCS(readers)
								b.Cleanup(func() {
									runtime.GOMAXPROCS(oldProcs)
								})

								var workerID atomic.Uint64
								b.SetParallelism(1)
								b.ReportAllocs()
								b.ResetTimer()
								b.RunParallel(func(pb *testing.PB) {
									id := int(workerID.Add(1) - 1)
									i := id * (benchSize / readers)
									for pb.Next() {
										m.Seek(keys[perm[i]])
										i++
										if i == benchSize {
											i = 0
										}
									}
								})
							})
						}
					})

					b.Run("Iterator/All", func(b *testing.B) {
						m := impl.Factory()
						benchFill(b, m, keys, val)

						b.ReportAllocs()
						for b.Loop() {
							count := 0
							c := m.Iterator(nil, nil)
							for {
								_, _, ok := c.Next()
								if !ok {
									break
								}
								count++
							}
							if count != benchSize {
								b.Fatalf("expected %d, got %d", benchSize, count)
							}
						}
						reportPerKey(b, benchSize)
					})

					b.Run("Iterator/ConcurrentR+W", func(b *testing.B) {
						benchConcurrentRW(b, impl.Factory, keys, val, func(m Memtable, i int) {
							it := m.Iterator(keys[perm[i]], nil)
							for range benchScanLen {
								if _, _, ok := it.Next(); !ok {
									return
								}
							}
						})
					})
				})
			}
		})
	}
}

// runs b.N reads across readers against one appending writer, reporting writes/s
func benchConcurrentRW(b *testing.B, factory func(...Option) Memtable, keys [][]byte, val []byte, read func(m Memtable, i int)) {
	benchSize := len(keys)
	for _, readers := range benchReaders {
		b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
			if readers+1 > runtime.NumCPU() {
				b.Skipf("needs %d CPUs, have %d", readers+1, runtime.NumCPU())
			}
			oldProcs := runtime.GOMAXPROCS(readers + 1)
			b.Cleanup(func() {
				runtime.GOMAXPROCS(oldProcs)
			})

			m := factory(WithCapacityBytes(2 * benchMemtableCapacity))
			benchFill(b, m, keys, val)

			start := make(chan struct{})
			var stop atomic.Bool
			var writes atomic.Uint64

			writerDone := make(chan error, 1)
			go func() {
				<-start

				for i := 0; i < benchSize && !stop.Load(); i++ {
					newKey := benchKey(benchSize + i)
					if err := m.Set(newKey, val); err != nil {
						writerDone <- err
						return
					}
					writes.Add(1)
				}

				writerDone <- nil
			}()

			var wg sync.WaitGroup
			wg.Add(readers)

			for r := range readers {
				// a fixed share of b.N per reader, no shared counter
				n := b.N / readers
				if r < b.N%readers {
					n++
				}

				offset := r * benchSize / readers

				go func(n, offset int) {
					defer wg.Done()
					<-start

					i := offset
					for range n {
						read(m, i)

						i++
						if i == benchSize {
							i = 0
						}
					}
				}(n, offset)
			}

			b.ResetTimer()
			started := time.Now()
			close(start)

			wg.Wait()

			b.StopTimer()
			elapsed := time.Since(started)

			stop.Store(true)
			err := <-writerDone

			if elapsed > 0 {
				b.ReportMetric(
					float64(writes.Load())/elapsed.Seconds(),
					"writes/s",
				)
			}

			if err != nil {
				b.Fatalf(
					"writer stopped after %d writes: %v",
					writes.Load(), err,
				)
			}
		})
	}
}

func benchKey(i int) []byte {
	return fmt.Appendf(nil, "key:%012d", i)
}

func benchValue(n int) []byte {
	v := make([]byte, n)
	rand.New(rand.NewSource(benchRandSeed)).Read(v)
	return v
}

func benchFill(tb testing.TB, m Memtable, keys [][]byte, val []byte) {
	tb.Helper()
	for i := range keys {
		if err := m.Set(keys[i], val); err != nil {
			tb.Fatalf("fill failed at %d/%d: %v", i, len(keys), err)
		}
	}
}

func benchKeys(tb testing.TB, entries int) [][]byte {
	tb.Helper()
	keys := make([][]byte, entries)
	for i := range keys {
		keys[i] = benchKey(i)
	}
	if len(keys) > 0 && len(keys[0]) != benchKeyLen {
		tb.Fatalf("benchKey width %d, want %d", len(keys[0]), benchKeyLen)
	}
	return keys
}

func makeBenchZipfTrace(keyCount int, skew float64) []int {
	rng := rand.New(rand.NewSource(benchRandSeed))
	rank := rng.Perm(keyCount) // decouple popularity from key order
	zipf := rand.NewZipf(rng, skew, 1, uint64(keyCount-1))
	trace := make([]int, benchZipfTraceSize)
	for i := range trace {
		trace[i] = rank[zipf.Uint64()]
	}
	return trace
}

func reportPerKey(b *testing.B, keysPerOp int) {
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*float64(keysPerOp)), "ns/op")
}
