package binfile

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// BenchmarkBinWriterWrite benchmarks regular binWriter write performance
func BenchmarkBinWriterWrite(b *testing.B) {
	root := GetTestDir("bench_bin_writer")
	_ = os.MkdirAll(root, 0755)
	defer CleanupTestDir(root)

	docSizes := []struct {
		name string
		docs []*Doc
	}{
		{"SmallDocs", CreateTestDocs(100)},
		{"MediumDocs", CreateTestDocs(50)},
		{"LargeDocs", CreateTestDocs(20)},
	}

	compressTypes := []int{NONE, GZIP}
	for _, compType := range compressTypes {
		compTypeName := GetCompressionTypeName(compType)
		for _, docSize := range docSizes {
			b.Run(fmt.Sprintf("Regular_%s_%s", compTypeName, docSize.name), func(b *testing.B) {
				testFile := filepath.Join(root, fmt.Sprintf("bench_reg_%s_%s.bin", compTypeName, docSize.name))
				writer := NewBinWriter(testFile, compType)
				err := writer.Open()
				if err != nil {
					b.Fatalf("Open failed: %v", err)
				}
				defer func() {
					_ = writer.Close()
					_ = os.Remove(testFile)
				}()

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for _, doc := range docSize.docs {
						_, err := writer.Write(doc)
						if err != nil {
							b.Fatalf("Write failed: %v", err)
						}
					}
				}
			})
		}
	}
}

// BenchmarkBufBinWriterWrite benchmarks bufBinWriter write performance
func BenchmarkBufBinWriterWrite(b *testing.B) {
	root := GetTestDir("bench_buf_writer")
	_ = os.MkdirAll(root, 0755)
	defer CleanupTestDir(root)

	docSizes := []struct {
		name string
		docs []*Doc
	}{
		{"SmallDocs", CreateTestDocs(100)},
		{"MediumDocs", CreateTestDocs(50)},
		{"LargeDocs", CreateTestDocs(20)},
	}

	bufferSizes := []int{16 * 1024, 64 * 1024, 256 * 1024}
	compressTypes := []int{NONE, GZIP}

	for _, compType := range compressTypes {
		compTypeName := GetCompressionTypeName(compType)
		for _, docSize := range docSizes {
			for _, bufSize := range bufferSizes {
				b.Run(fmt.Sprintf("Buffered_%s_%s_%dKB", compTypeName, docSize.name, bufSize/1024), func(b *testing.B) {
					testFile := filepath.Join(root, fmt.Sprintf("bench_buf_%s_%s_%d.bin", compTypeName, docSize.name, bufSize))
					writer := NewBufBinWriter(testFile, compType, bufSize)
					err := writer.Open()
					if err != nil {
						b.Fatalf("Open failed: %v", err)
					}
					defer func() {
						_ = writer.Close()
						_ = os.Remove(testFile)
					}()

					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						for _, doc := range docSize.docs {
							_, err := writer.Write(doc)
							if err != nil {
								b.Fatalf("Write failed: %v", err)
							}
						}
					}
				})
			}
		}
	}
}

// BenchmarkWriterComparison compares regular and buffered writer performance
func BenchmarkWriterComparison(b *testing.B) {
	root := GetTestDir("bench_comparison")
	_ = os.MkdirAll(root, 0755)
	defer CleanupTestDir(root)

	testDocs := CreateTestDocs(100)
	compressTypes := []int{NONE, GZIP}

	for _, compType := range compressTypes {
		compTypeName := GetCompressionTypeName(compType)
		b.Run(compTypeName, func(b *testing.B) {
			// Regular writer
			b.Run("Regular", func(b *testing.B) {
				testFile := filepath.Join(root, fmt.Sprintf("bench_reg_%s.bin", compTypeName))
				writer := NewBinWriter(testFile, compType)
				err := writer.Open()
				if err != nil {
					b.Fatalf("Open failed: %v", err)
				}
				defer func() {
					_ = writer.Close()
					_ = os.Remove(testFile)
				}()

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for _, doc := range testDocs {
						_, err := writer.Write(doc)
						if err != nil {
							b.Fatalf("Write failed: %v", err)
						}
					}
				}
			})

			// Buffered writer with default size
			b.Run("Buffered_64KB", func(b *testing.B) {
				testFile := filepath.Join(root, fmt.Sprintf("bench_buf_%s.bin", compTypeName))
				writer := NewBufBinWriter(testFile, compType, 64*1024)
				err := writer.Open()
				if err != nil {
					b.Fatalf("Open failed: %v", err)
				}
				defer func() {
					_ = writer.Close()
					_ = os.Remove(testFile)
				}()

				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					for _, doc := range testDocs {
						_, err := writer.Write(doc)
						if err != nil {
							b.Fatalf("Write failed: %v", err)
						}
					}
				}
			})
		})
	}
}

// TestBufBinWriterPerformance tests performance improvement of buffered writer
func TestBufBinWriterPerformance(t *testing.T) {
	root := GetTestDir("test_perf")
	_ = os.MkdirAll(root, 0755)
	defer CleanupTestDir(root)

	testDocs := CreateTestDocs(500)
	compressTypes := []int{NONE, GZIP}

	for _, compType := range compressTypes {
		compTypeName := GetCompressionTypeName(compType)
		t.Run(compTypeName, func(t *testing.T) {
			// Test regular writer
			regFile := filepath.Join(root, fmt.Sprintf("perf_reg_%s.bin", compTypeName))
			regWriter := NewBinWriter(regFile, compType)
			err := regWriter.Open()
			if err != nil {
				t.Fatalf("Open regular writer failed: %v", err)
			}

			var m1, m2 runtime.MemStats
			runtime.GC()
			runtime.ReadMemStats(&m1)
			start := time.Now()

			for _, doc := range testDocs {
				_, err := regWriter.Write(doc)
				if err != nil {
					t.Fatalf("Write failed: %v", err)
				}
			}

			regDuration := time.Since(start)
			err = regWriter.Close()
			if err != nil {
				t.Fatalf("Close failed: %v", err)
			}

			runtime.GC()
			runtime.ReadMemStats(&m2)
			regAllocs := m2.Mallocs - m1.Mallocs
			regTime := regDuration

			// Test buffered writer
			bufFile := filepath.Join(root, fmt.Sprintf("perf_buf_%s.bin", compTypeName))
			bufWriter := NewBufBinWriter(bufFile, compType, 64*1024)
			err = bufWriter.Open()
			if err != nil {
				t.Fatalf("Open buffered writer failed: %v", err)
			}

			runtime.GC()
			runtime.ReadMemStats(&m1)
			start = time.Now()

			for _, doc := range testDocs {
				_, err := bufWriter.Write(doc)
				if err != nil {
					t.Fatalf("Write failed: %v", err)
				}
			}

			bufDuration := time.Since(start)
			err = bufWriter.Close()
			if err != nil {
				t.Fatalf("Close failed: %v", err)
			}

			runtime.GC()
			runtime.ReadMemStats(&m2)
			bufAllocs := m2.Mallocs - m1.Mallocs
			bufTime := bufDuration

			// Log results
			t.Logf("Compression: %s", compTypeName)
			t.Logf("Regular Writer:")
			t.Logf("  Time: %v", regTime)
			t.Logf("  Allocations: %d", regAllocs)
			t.Logf("Buffered Writer:")
			t.Logf("  Time: %v", bufTime)
			t.Logf("  Allocations: %d", bufAllocs)
			t.Logf("Performance Improvement: %.2f%%", float64(regTime-bufTime)/float64(regTime)*100)
			t.Logf("Allocation Reduction: %.2f%%", float64(regAllocs-bufAllocs)/float64(regAllocs)*100)

			// Verify both files are readable and have same content
			// Note: For compressed types, we only verify file existence and size
			// as the format may differ from uncompressed format
			if compType == NONE {
				regReader, err := NewBinReader(regFile, compType)
				if err != nil {
					t.Fatalf("Read regular file failed: %v", err)
				}
				defer regReader.Close()

				bufReader, err := NewBinReader(bufFile, compType)
				if err != nil {
					t.Fatalf("Read buffered file failed: %v", err)
				}
				defer bufReader.Close()

				regCount := regReader.Count(&CountOption{Offset: 0, End: -1, WorkerCount: 1, SkipError: false})
				bufCount := bufReader.Count(&CountOption{Offset: 0, End: -1, WorkerCount: 1, SkipError: false})

				if regCount != bufCount {
					t.Errorf("Document count mismatch: regular=%d, buffered=%d", regCount, bufCount)
				}

				if regCount != int64(len(testDocs)) {
					t.Errorf("Document count incorrect: expected=%d, actual=%d", len(testDocs), regCount)
				}
			} else {
				// For compressed files, just verify they exist and have content
				regStat, err := os.Stat(regFile)
				if err != nil {
					t.Errorf("Regular file stat failed: %v", err)
				} else if regStat.Size() == 0 {
					t.Errorf("Regular file is empty")
				}

				bufStat, err := os.Stat(bufFile)
				if err != nil {
					t.Errorf("Buffered file stat failed: %v", err)
				} else if bufStat.Size() == 0 {
					t.Errorf("Buffered file is empty")
				}
			}
		})
	}
}

// BenchmarkWriterBufferSizes benchmarks different buffer sizes
func BenchmarkWriterBufferSizes(b *testing.B) {
	root := GetTestDir("bench_buf_sizes")
	_ = os.MkdirAll(root, 0755)
	defer CleanupTestDir(root)

	testDocs := CreateTestDocs(100)
	bufferSizes := []int{4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024, 1024 * 1024}

	for _, bufSize := range bufferSizes {
		b.Run(fmt.Sprintf("Buffer_%dKB", bufSize/1024), func(b *testing.B) {
			testFile := filepath.Join(root, fmt.Sprintf("bench_%d.bin", bufSize))
			writer := NewBufBinWriter(testFile, NONE, bufSize)
			err := writer.Open()
			if err != nil {
				b.Fatalf("Open failed: %v", err)
			}
			defer func() {
				_ = writer.Close()
				_ = os.Remove(testFile)
			}()

			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				for _, doc := range testDocs {
					_, err := writer.Write(doc)
					if err != nil {
						b.Fatalf("Write failed: %v", err)
					}
				}
			}
		})
	}
}

// BenchmarkWriterConcurrentWrite benchmarks concurrent write performance
func BenchmarkWriterConcurrentWrite(b *testing.B) {
	root := GetTestDir("bench_concurrent")
	_ = os.MkdirAll(root, 0755)
	defer CleanupTestDir(root)

	testDocs := CreateTestDocs(50)
	goroutines := []int{1, 2, 4, 8}

	for _, numGoroutines := range goroutines {
		b.Run(fmt.Sprintf("Goroutines_%d", numGoroutines), func(b *testing.B) {
			// Regular writer
			b.Run("Regular", func(b *testing.B) {
				testFile := filepath.Join(root, fmt.Sprintf("bench_reg_%d.bin", numGoroutines))
				writer := NewBinWriter(testFile, NONE)
				err := writer.Open()
				if err != nil {
					b.Fatalf("Open failed: %v", err)
				}
				defer func() {
					_ = writer.Close()
					_ = os.Remove(testFile)
				}()

				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						for _, doc := range testDocs {
							_, err := writer.Write(doc)
							if err != nil {
								b.Fatalf("Write failed: %v", err)
							}
						}
					}
				})
			})

			// Buffered writer
			b.Run("Buffered", func(b *testing.B) {
				testFile := filepath.Join(root, fmt.Sprintf("bench_buf_%d.bin", numGoroutines))
				writer := NewBufBinWriter(testFile, NONE, 64*1024)
				err := writer.Open()
				if err != nil {
					b.Fatalf("Open failed: %v", err)
				}
				defer func() {
					_ = writer.Close()
					_ = os.Remove(testFile)
				}()

				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						for _, doc := range testDocs {
							_, err := writer.Write(doc)
							if err != nil {
								b.Fatalf("Write failed: %v", err)
							}
						}
					}
				})
			})
		})
	}
}
