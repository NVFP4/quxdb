package memtable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
)

const (
	benchMemtableCapacity = 32 << 20
	benchUserKeyLen       = 16
	benchZipfTraceSize    = 64 << 10
	benchValueSizeN       = 100_000
)

var (
	benchSizes      = []int{100_000, 300_000}
	benchReaders    = []int{4, 8, 16}
	benchValueSizes = []int{16, 128, 1024}
	benchZipfSkews  = []float64{1.1, 1.3}
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
	val := []byte("value")

	for _, impl := range benchImpls {
		b.Run("impl="+impl.name, func(b *testing.B) {
			for _, benchSize := range benchSizes {
				b.Run(fmt.Sprintf("N=%d", benchSize), func(b *testing.B) {
					b.Run("Set/Unique/Seq", func(b *testing.B) {
						keys := make([][]byte, benchSize)
						for i := range benchSize {
							keys[i] = fmt.Appendf(nil, "unique-seq:%016d", i)
						}

						b.ReportAllocs()
						for b.Loop() {
							b.StopTimer()
							m := impl.Factory()
							b.StartTimer()
							var err error
							for i := range benchSize {
								if err = m.Set(keys[i], val); err != nil {
									break
								}
							}
							b.StopTimer()
							if err != nil {
								b.Fatal(err)
							}
							b.StartTimer()
						}
						reportPerKey(b, benchSize)
					})

					b.Run("Set/Unique/Rand", func(b *testing.B) {
						rng := rand.New(rand.NewSource(42))
						keys := make([][]byte, benchSize)
						for i := range benchSize {
							keys[i] = fmt.Appendf(nil, "unique-rand:%016x:%016d", rng.Uint64(), i)
						}

						b.ReportAllocs()
						for b.Loop() {
							b.StopTimer()
							m := impl.Factory()
							b.StartTimer()
							var err error
							for i := range benchSize {
								if err = m.Set(keys[i], val); err != nil {
									break
								}
							}
							b.StopTimer()
							if err != nil {
								b.Fatal(err)
							}
							b.StartTimer()
						}
						reportPerKey(b, benchSize)
					})

					b.Run("Set/Overwrite/Seq", func(b *testing.B) {
						m := impl.Factory()
						keys := make([][]byte, benchSize)
						for i := range benchSize {
							keys[i] = fmt.Appendf(nil, "unique-seq:%016d", i)
							m.Set(keys[i], val)
						}

						b.ReportAllocs()
						i := 0
						for b.Loop() {
							if err := m.Set(keys[i], val); err != nil {
								b.StopTimer()
								b.Fatal(err)
							}
							i++
							if i == len(keys) {
								i = 0
							}
						}
					})

					b.Run("Set/Overwrite/Rand", func(b *testing.B) {
						m := impl.Factory()
						rngLocal := rand.New(rand.NewSource(42))
						keys := make([][]byte, benchSize)
						for i := range benchSize {
							keys[i] = fmt.Appendf(nil, "unique-rand:%016x:%016d", rngLocal.Uint64(), i)
							m.Set(keys[i], val)
						}
						perm := rngLocal.Perm(benchSize)

						b.ReportAllocs()
						i := 0
						for b.Loop() {
							if err := m.Set(keys[perm[i]], val); err != nil {
								b.StopTimer()
								b.Fatal(err)
							}
							i++
							if i == len(keys) {
								i = 0
							}
						}
					})

					b.Run("Get/Seq", func(b *testing.B) {
						keys := make([][]byte, benchSize)
						for i := range benchSize {
							keys[i] = fmt.Appendf(nil, "unique-seq:%016d", i)
						}

						m := impl.Factory()
						for i := range benchSize {
							m.Set(keys[i], val)
						}

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
						rngLocal := rand.New(rand.NewSource(42))
						keys := make([][]byte, benchSize)
						for i := range benchSize {
							keys[i] = fmt.Appendf(nil, "unique-rand:%016x:%016d", rngLocal.Uint64(), i)
						}

						perm := rngLocal.Perm(benchSize)

						m := impl.Factory()
						for i := range benchSize {
							m.Set(keys[i], val)
						}

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

					b.Run("Get/Miss", func(b *testing.B) {
						keys := make([][]byte, benchSize)
						missing := make([][]byte, benchSize)
						m := impl.Factory()
						for i := range benchSize {
							keys[i] = fmt.Appendf(nil, "unique-seq:%016d", i)
							missing[i] = fmt.Appendf(nil, "missing:%016d", i)
							if err := m.Set(keys[i], val); err != nil {
								b.Fatal(err)
							}
						}

						b.ReportAllocs()
						i := 0
						for b.Loop() {
							m.Get(missing[i])
							i++
							if i == benchSize {
								i = 0
							}
						}
					})

					b.Run("Get/Skewed", func(b *testing.B) {
						rng := rand.New(rand.NewSource(42))
						keys := make([][]byte, benchSize)
						m := impl.Factory()
						for i := range benchSize {
							keys[i] = fmt.Appendf(nil, "unique-rand:%016x:%016d", rng.Uint64(), i)
							if err := m.Set(keys[i], val); err != nil {
								b.Fatal(err)
							}
						}

						for _, skew := range benchZipfSkews {
							b.Run(fmt.Sprintf("s=%.1f", skew), func(b *testing.B) {
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

					b.Run("Get/Concurrency", func(b *testing.B) {
						rng := rand.New(rand.NewSource(42))
						keys := make([][]byte, benchSize)
						m := impl.Factory()
						for i := range benchSize {
							keys[i] = fmt.Appendf(nil, "unique-rand:%016x:%016d", rng.Uint64(), i)
							if err := m.Set(keys[i], val); err != nil {
								b.Fatal(err)
							}
						}
						perm := rng.Perm(benchSize)

						for _, readers := range benchReaders {
							b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
								previousProcs := runtime.GOMAXPROCS(readers)
								defer runtime.GOMAXPROCS(previousProcs)

								var workerID atomic.Uint64
								b.SetParallelism(1)
								b.ReportAllocs()
								b.RunParallel(func(pb *testing.PB) {
									i := int(workerID.Add(1)-1) * (benchSize / readers)
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

					b.Run("Seek/Seq", func(b *testing.B) {
						keys := make([][]byte, benchSize)
						for i := range benchSize {
							keys[i] = fmt.Appendf(nil, "unique-seq:%016d", i)
						}

						m := impl.Factory()
						for i := range benchSize {
							m.Set(keys[i], val)
						}

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
						rngLocal := rand.New(rand.NewSource(42))
						keys := make([][]byte, benchSize)
						for i := range benchSize {
							keys[i] = fmt.Appendf(nil, "unique-rand:%016x:%016d", rngLocal.Uint64(), i)
						}

						perm := rngLocal.Perm(benchSize)

						m := impl.Factory()
						for i := range benchSize {
							m.Set(keys[i], val)
						}

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

					b.Run("Seek/Concurrency", func(b *testing.B) {
						rng := rand.New(rand.NewSource(42))
						keys := make([][]byte, benchSize)
						m := impl.Factory()
						for i := range benchSize {
							keys[i] = fmt.Appendf(nil, "unique-rand:%016x:%016d", rng.Uint64(), i)
							if err := m.Set(keys[i], val); err != nil {
								b.Fatal(err)
							}
						}
						perm := rng.Perm(benchSize)

						for _, readers := range benchReaders {
							b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
								previousProcs := runtime.GOMAXPROCS(readers)
								defer runtime.GOMAXPROCS(previousProcs)

								var workerID atomic.Uint64
								b.SetParallelism(1)
								b.ReportAllocs()
								b.RunParallel(func(pb *testing.PB) {
									i := int(workerID.Add(1)-1) * (benchSize / readers)
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

					b.Run("Cursor/All", func(b *testing.B) {
						m := impl.Factory()
						for i := range benchSize {
							m.Set(fmt.Appendf(nil, "unique-seq:%016d", i), val)
						}

						b.ReportAllocs()
						for b.Loop() {
							count := 0
							c := m.Cursor(nil, nil)
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
					})

				})
			}
		})
	}
}

func BenchmarkMemtableMVCC(b *testing.B) {
	val := []byte("value")

	for _, impl := range benchImpls {
		b.Run("impl="+impl.name, func(b *testing.B) {
			for _, benchSize := range benchSizes {
				b.Run(fmt.Sprintf("N=%d", benchSize), func(b *testing.B) {
					b.Run("Set/Unique/Seq", func(b *testing.B) {
						keys := make([][]byte, benchSize)
						for i := range benchSize {
							userKey := fmt.Appendf(nil, "%016d", i+1)
							keys[i] = makeMVCCKey(userKey, uint64(i)+1, mvccOpSet)
						}

						b.ReportAllocs()
						for b.Loop() {
							b.StopTimer()
							m := impl.Factory(WithComparator(compareMVCCKey))
							b.StartTimer()
							var err error
							for i := range benchSize {
								if err = m.Set(keys[i], val); err != nil {
									break
								}
							}
							b.StopTimer()
							if err != nil {
								b.Fatal(err)
							}
							b.StartTimer()
						}
						reportPerKey(b, benchSize)
					})

					b.Run("Set/Unique/Rand", func(b *testing.B) {
						rng := rand.New(rand.NewSource(42))
						keys := make([][]byte, benchSize)
						for i := range benchSize {
							userKey := make([]byte, benchUserKeyLen)
							binary.BigEndian.PutUint64(userKey[:8], rng.Uint64())
							binary.BigEndian.PutUint64(userKey[8:16], uint64(i))
							keys[i] = makeMVCCKey(userKey, uint64(i)+1, mvccOpSet)
						}

						b.ReportAllocs()
						for b.Loop() {
							b.StopTimer()
							m := impl.Factory(WithComparator(compareMVCCKey))
							b.StartTimer()
							var err error
							for i := range benchSize {
								if err = m.Set(keys[i], val); err != nil {
									break
								}
							}
							b.StopTimer()
							if err != nil {
								b.Fatal(err)
							}
							b.StartTimer()
						}
						reportPerKey(b, benchSize)
					})

					b.Run("Set/MultiVersion", func(b *testing.B) {
						for _, vpk := range []int{8, 64, 512} {
							b.Run(fmt.Sprintf("vpk=%d", vpk), func(b *testing.B) {
								hotKeys := benchSize / vpk
								rng := rand.New(rand.NewSource(42))
								keys := make([][]byte, benchSize)
								for i := range benchSize {
									userKey := fmt.Appendf(nil, "hot:%08d", rng.Intn(hotKeys))
									keys[i] = makeMVCCKey(userKey, uint64(i)+1, mvccOpSet)
								}

								b.ReportAllocs()
								for b.Loop() {
									b.StopTimer()
									m := impl.Factory(WithComparator(compareMVCCKey))
									b.StartTimer()
									var err error
									for i := range benchSize {
										if err = m.Set(keys[i], val); err != nil {
											break
										}
									}
									b.StopTimer()
									if err != nil {
										b.Fatal(err)
									}
									b.StartTimer()
								}
								reportPerKey(b, benchSize)
							})
						}
					})

					b.Run("Get/Seq", func(b *testing.B) {
						m := impl.Factory(WithComparator(compareMVCCKey))
						keys := make([][]byte, benchSize)
						for i := range benchSize {
							keys[i] = makeMVCCKey(fmt.Appendf(nil, "%016d", i+1), uint64(i)+1, mvccOpSet)
						}

						for i := range benchSize {
							m.Set(keys[i], val)
						}

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
						m := impl.Factory(WithComparator(compareMVCCKey))
						rngLocal := rand.New(rand.NewSource(42))
						keys := make([][]byte, benchSize)
						for i := range benchSize {
							userKey := make([]byte, benchUserKeyLen)
							binary.BigEndian.PutUint64(userKey[:8], rngLocal.Uint64())
							binary.BigEndian.PutUint64(userKey[8:16], uint64(i))
							keys[i] = makeMVCCKey(userKey, uint64(i)+1, mvccOpSet)
						}

						for i := range benchSize {
							m.Set(keys[i], val)
						}

						perm := rngLocal.Perm(benchSize)

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

					b.Run("Get/Miss", func(b *testing.B) {
						m := impl.Factory(WithComparator(compareMVCCKey))
						keys := make([][]byte, benchSize)
						missing := make([][]byte, benchSize)
						for i := range benchSize {
							keys[i] = makeMVCCKey(fmt.Appendf(nil, "%016d", i), uint64(i)+1, mvccOpSet)
							missing[i] = makeMVCCKey(fmt.Appendf(nil, "missing:%016d", i), uint64(i)+1, mvccOpSet)
							if err := m.Set(keys[i], val); err != nil {
								b.Fatal(err)
							}
						}

						b.ReportAllocs()
						i := 0
						for b.Loop() {
							m.Get(missing[i])
							i++
							if i == benchSize {
								i = 0
							}
						}
					})

					b.Run("Get/Skewed", func(b *testing.B) {
						m := impl.Factory(WithComparator(compareMVCCKey))
						rng := rand.New(rand.NewSource(42))
						keys := make([][]byte, benchSize)
						for i := range benchSize {
							userKey := make([]byte, benchUserKeyLen)
							binary.BigEndian.PutUint64(userKey[:8], rng.Uint64())
							binary.BigEndian.PutUint64(userKey[8:], uint64(i))
							keys[i] = makeMVCCKey(userKey, uint64(i)+1, mvccOpSet)
							if err := m.Set(keys[i], val); err != nil {
								b.Fatal(err)
							}
						}

						for _, skew := range benchZipfSkews {
							b.Run(fmt.Sprintf("s=%.1f", skew), func(b *testing.B) {
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

					b.Run("Get/Concurrency", func(b *testing.B) {
						rng := rand.New(rand.NewSource(42))
						keys := make([][]byte, benchSize)
						m := impl.Factory(WithComparator(compareMVCCKey))
						for i := range benchSize {
							userKey := make([]byte, benchUserKeyLen)
							binary.BigEndian.PutUint64(userKey[:8], rng.Uint64())
							binary.BigEndian.PutUint64(userKey[8:], uint64(i))
							keys[i] = makeMVCCKey(userKey, uint64(i)+1, mvccOpSet)
							if err := m.Set(keys[i], val); err != nil {
								b.Fatal(err)
							}
						}
						perm := rng.Perm(benchSize)

						for _, readers := range benchReaders {
							b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
								previousProcs := runtime.GOMAXPROCS(readers)
								defer runtime.GOMAXPROCS(previousProcs)

								var workerID atomic.Uint64
								b.SetParallelism(1)
								b.ReportAllocs()
								b.RunParallel(func(pb *testing.PB) {
									i := int(workerID.Add(1)-1) * (benchSize / readers)
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

					b.Run("Seek/Seq", func(b *testing.B) {
						m := impl.Factory(WithComparator(compareMVCCKey))
						keys := make([][]byte, benchSize)
						seekKeys := make([][]byte, benchSize)
						for i := range benchSize {
							userKey := fmt.Appendf(nil, "%016d", i+1)
							keys[i] = makeMVCCKey(userKey, uint64(i)+1, mvccOpSet)
							seekKeys[i] = makeMVCCKey(userKey, uint64(benchSize)+1, mvccOpSeekMax)
						}

						for i := range benchSize {
							m.Set(keys[i], val)
						}

						b.ReportAllocs()
						i := 0
						for b.Loop() {
							m.Seek(seekKeys[i])
							i++
							if i == benchSize {
								i = 0
							}
						}
					})

					b.Run("Seek/Rand", func(b *testing.B) {
						m := impl.Factory(WithComparator(compareMVCCKey))
						rngLocal := rand.New(rand.NewSource(42))
						keys := make([][]byte, benchSize)
						seekKeys := make([][]byte, benchSize)
						for i := range benchSize {
							userKey := make([]byte, benchUserKeyLen)
							binary.BigEndian.PutUint64(userKey[:8], rngLocal.Uint64())
							binary.BigEndian.PutUint64(userKey[8:16], uint64(i))
							keys[i] = makeMVCCKey(userKey, uint64(i)+1, mvccOpSet)
							seekKeys[i] = makeMVCCKey(userKey, uint64(benchSize)+1, mvccOpSeekMax)
						}
						for i := range benchSize {
							m.Set(keys[i], val)
						}

						perm := rngLocal.Perm(benchSize)

						b.ReportAllocs()
						i := 0
						for b.Loop() {
							m.Seek(seekKeys[perm[i]])
							i++
							if i == benchSize {
								i = 0
							}
						}
					})

					b.Run("Seek/Concurrency", func(b *testing.B) {
						rng := rand.New(rand.NewSource(42))
						keys := make([][]byte, benchSize)
						seekKeys := make([][]byte, benchSize)
						m := impl.Factory(WithComparator(compareMVCCKey))
						for i := range benchSize {
							userKey := make([]byte, benchUserKeyLen)
							binary.BigEndian.PutUint64(userKey[:8], rng.Uint64())
							binary.BigEndian.PutUint64(userKey[8:], uint64(i))
							keys[i] = makeMVCCKey(userKey, uint64(i)+1, mvccOpSet)
							seekKeys[i] = makeMVCCKey(userKey, uint64(benchSize)+1, mvccOpSeekMax)
							if err := m.Set(keys[i], val); err != nil {
								b.Fatal(err)
							}
						}
						perm := rng.Perm(benchSize)

						for _, readers := range benchReaders {
							b.Run(fmt.Sprintf("readers=%d", readers), func(b *testing.B) {
								previousProcs := runtime.GOMAXPROCS(readers)
								defer runtime.GOMAXPROCS(previousProcs)

								var workerID atomic.Uint64
								b.SetParallelism(1)
								b.ReportAllocs()
								b.RunParallel(func(pb *testing.PB) {
									i := int(workerID.Add(1)-1) * (benchSize / readers)
									for pb.Next() {
										m.Seek(seekKeys[perm[i]])
										i++
										if i == benchSize {
											i = 0
										}
									}
								})
							})
						}
					})

					b.Run("Cursor/All", func(b *testing.B) {
						m := impl.Factory(WithComparator(compareMVCCKey))
						for i := range benchSize {
							userKey := fmt.Appendf(nil, "%016d", i+1)
							m.Set(makeMVCCKey(userKey, uint64(i)+1, mvccOpSet), val)
						}

						b.ReportAllocs()
						for b.Loop() {
							count := 0
							c := m.Cursor(nil, nil)
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
					})

				})
			}
		})
	}
}

func BenchmarkMemtableMVCCValueSize(b *testing.B) {
	for _, impl := range benchImpls {
		b.Run("impl="+impl.name, func(b *testing.B) {
			for _, valueSize := range benchValueSizes {
				b.Run(fmt.Sprintf("valueBytes=%d", valueSize), func(b *testing.B) {
					rng := rand.New(rand.NewSource(42))
					keys := make([][]byte, benchValueSizeN)
					capacity := benchMemtableCapacity + benchValueSizeN*valueSize
					for i := range benchValueSizeN {
						userKey := make([]byte, benchUserKeyLen)
						binary.BigEndian.PutUint64(userKey[:8], rng.Uint64())
						binary.BigEndian.PutUint64(userKey[8:], uint64(i))
						keys[i] = makeMVCCKey(userKey, uint64(i)+1, mvccOpSet)
						capacity += len(keys[i])
					}
					value := bytes.Repeat([]byte{0xab}, valueSize)

					b.Run("Set/Unique/Rand", func(b *testing.B) {
						b.ReportAllocs()
						b.SetBytes(int64(benchValueSizeN * valueSize))
						for b.Loop() {
							b.StopTimer()
							m := impl.Factory(
								WithCapacityBytes(capacity),
								WithComparator(compareMVCCKey),
							)
							b.StartTimer()
							var err error
							for i := range benchValueSizeN {
								if err = m.Set(keys[i], value); err != nil {
									break
								}
							}
							b.StopTimer()
							if err != nil {
								b.Fatal(err)
							}
							b.StartTimer()
						}
						reportPerKey(b, benchValueSizeN)
					})

					b.Run("Get/Rand", func(b *testing.B) {
						m := impl.Factory(
							WithCapacityBytes(capacity),
							WithComparator(compareMVCCKey),
						)
						for i := range benchValueSizeN {
							if err := m.Set(keys[i], value); err != nil {
								b.Fatal(err)
							}
						}
						perm := rng.Perm(benchValueSizeN)

						b.ReportAllocs()
						i := 0
						for b.Loop() {
							m.Get(keys[perm[i]])
							i++
							if i == benchValueSizeN {
								i = 0
							}
						}
					})
				})
			}
		})
	}
}

func makeBenchZipfTrace(keyCount int, skew float64) []int {
	rng := rand.New(rand.NewSource(42))
	zipf := rand.NewZipf(rng, skew, 1, uint64(keyCount-1))
	trace := make([]int, benchZipfTraceSize)
	for i := range trace {
		trace[i] = int(zipf.Uint64())
	}
	return trace
}

func reportPerKey(b *testing.B, keysPerOp int) {
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/(float64(b.N)*float64(keysPerOp)), "ns/op")
}
