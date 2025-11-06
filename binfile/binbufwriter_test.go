package binfile

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

// TestBufBinWriterCreation tests bufBinWriter creation
func TestBufBinWriterCreation(t *testing.T) {
	root := GetTestDir("test_buf_bin_writer")
	_ = os.MkdirAll(root, 0755)
	defer CleanupTestDir(root)

	compressTypes := GetAllCompressionTypes()
	for _, compressType := range compressTypes {
		compression := GetCompressionTypeName(compressType)
		t.Run(compression, func(t *testing.T) {
			// Test with default buffer size
			tmpFile := filepath.Join(root, fmt.Sprintf("test_default_%s.bin", compression))
			writer := NewBufBinWriter(tmpFile, compressType, 0)
			err := writer.Open()
			if err != nil {
				t.Fatalf("BufBinWriter Open failed: %v", err)
			}
			defer func() {
				_ = writer.Close()
			}()

			// Test with custom buffer size
			tmpFile2 := filepath.Join(root, fmt.Sprintf("test_custom_%s.bin", compression))
			writer2 := NewBufBinWriter(tmpFile2, compressType, 128*1024)
			err = writer2.Open()
			if err != nil {
				t.Fatalf("BufBinWriter Open with custom size failed: %v", err)
			}
			defer func() {
				_ = writer2.Close()
			}()

			// Verify files were created
			if _, err := os.Stat(tmpFile); os.IsNotExist(err) {
				t.Fatalf("output file was not created: %s", tmpFile)
			}
			if _, err := os.Stat(tmpFile2); os.IsNotExist(err) {
				t.Fatalf("output file was not created: %s", tmpFile2)
			}
		})
	}
}

// TestBufBinWriterWriteRead tests writing and reading with bufBinWriter
func TestBufBinWriterWriteRead(t *testing.T) {
	root := GetTestDir("test_buf_writer_rw")
	_ = os.MkdirAll(root, 0755)
	defer CleanupTestDir(root)

	testDocs := []*Doc{
		{Key: []byte("small"), Content: []byte("Hello World")},
		{Key: []byte("medium"), Content: []byte(RandStringBytesMaskImprSrc(1024))},
		{Key: []byte("large"), Content: []byte(RandStringBytesMaskImprSrc(8192))},
		{Key: []byte("binary"), Content: []byte{0x00, 0x01, 0xFF, 0xFE, 0x7F, 0x80}},
	}

	compressTypes := GetAllCompressionTypes()
	for _, compressType := range compressTypes {
		compression := GetCompressionTypeName(compressType)
		t.Run(compression, func(t *testing.T) {
			testFile := filepath.Join(root, fmt.Sprintf("test_%s.bin", compression))

			// Write documents using bufBinWriter
			writer := NewBufBinWriter(testFile, compressType, 64*1024)
			err := writer.Open()
			if err != nil {
				t.Fatalf("Open failed: %v", err)
			}

			for _, doc := range testDocs {
				_, err := writer.Write(doc)
				if err != nil {
					t.Fatalf("Write failed: %v", err)
				}
			}

			err = writer.Close()
			if err != nil {
				t.Fatalf("Close failed: %v", err)
			}

			// Read and verify documents
			reader, err := NewBinReader(testFile, compressType)
			if err != nil {
				t.Fatalf("NewBinReader failed: %v", err)
			}
			defer reader.Close()

			for i, expectedDoc := range testDocs {
				pos := reader.Search(SearchOption{
					Key:    fmt.Sprintf("^%s$", string(expectedDoc.Key)),
					Skip:   1,
					Offset: 0,
				})

				if pos < 0 {
					t.Fatalf("Search failed for document %d", i)
				}

				actualDoc, err := reader.Read(pos, true)
				if err != nil || actualDoc == nil {
					t.Fatalf("Read document failed: index %d, %v", i, err)
				}

				if !bytes.Equal(expectedDoc.Key, actualDoc.Key) || !bytes.Equal(expectedDoc.Content, actualDoc.Content) {
					t.Errorf("Document mismatch: index %d\nExpected: Key=%s, ContentLen=%d\nActual: Key=%s, ContentLen=%d",
						i, string(expectedDoc.Key), len(expectedDoc.Content), string(actualDoc.Key), len(actualDoc.Content))
				}
			}
		})
	}
}

// TestBufBinWriterFlush tests manual buffer flushing
func TestBufBinWriterFlush(t *testing.T) {
	root := GetTestDir("test_buf_writer_flush")
	_ = os.MkdirAll(root, 0755)
	defer CleanupTestDir(root)

	testFile := filepath.Join(root, "test_flush.bin")
	writer := NewBufBinWriter(testFile, NONE, 64*1024)
	err := writer.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() {
		_ = writer.Close()
	}()

	// Write a document
	doc := &Doc{Key: []byte("test"), Content: []byte("test content")}
	_, err = writer.Write(doc)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Test Flush method (if available)
	if bufWriter, ok := writer.(*bufBinWriter); ok {
		err = bufWriter.Flush()
		if err != nil {
			t.Fatalf("Flush failed: %v", err)
		}

		// Verify file exists and has content
		stat, err := os.Stat(testFile)
		if err != nil {
			t.Fatalf("File stat failed: %v", err)
		}
		if stat.Size() == 0 {
			t.Fatalf("File is empty after flush")
		}
	}
}

// TestBufBinWriterCompatibility tests compatibility with binWriter
func TestBufBinWriterCompatibility(t *testing.T) {
	root := GetTestDir("test_buf_writer_compat")
	_ = os.MkdirAll(root, 0755)
	defer CleanupTestDir(root)

	testDocs := CreateTestDocs(10)

	// Write with bufBinWriter
	bufFile := filepath.Join(root, "buf_writer.bin")
	bufWriter := NewBufBinWriter(bufFile, NONE, 64*1024)
	err := bufWriter.Open()
	if err != nil {
		t.Fatalf("Open bufWriter failed: %v", err)
	}
	for _, doc := range testDocs {
		_, err := bufWriter.Write(doc)
		if err != nil {
			t.Fatalf("Write to bufWriter failed: %v", err)
		}
	}
	err = bufWriter.Close()
	if err != nil {
		t.Fatalf("Close bufWriter failed: %v", err)
	}

	// Write with regular binWriter
	regFile := filepath.Join(root, "reg_writer.bin")
	regWriter := NewBinWriter(regFile, NONE)
	err = regWriter.Open()
	if err != nil {
		t.Fatalf("Open regWriter failed: %v", err)
	}
	for _, doc := range testDocs {
		_, err := regWriter.Write(doc)
		if err != nil {
			t.Fatalf("Write to regWriter failed: %v", err)
		}
	}
	err = regWriter.Close()
	if err != nil {
		t.Fatalf("Close regWriter failed: %v", err)
	}

	// Both files should be readable
	bufReader, err := NewBinReader(bufFile, NONE)
	if err != nil {
		t.Fatalf("Read bufFile failed: %v", err)
	}
	defer bufReader.Close()

	regReader, err := NewBinReader(regFile, NONE)
	if err != nil {
		t.Fatalf("Read regFile failed: %v", err)
	}
	defer regReader.Close()

	// Both should have same document count
	bufCount := bufReader.Count(&CountOption{Offset: 0, End: -1, WorkerCount: 1, SkipError: false})
	regCount := regReader.Count(&CountOption{Offset: 0, End: -1, WorkerCount: 1, SkipError: false})

	if bufCount != regCount {
		t.Errorf("Document count mismatch: bufWriter=%d, regWriter=%d", bufCount, regCount)
	}

	if bufCount != int64(len(testDocs)) {
		t.Errorf("Document count incorrect: expected=%d, actual=%d", len(testDocs), bufCount)
	}
}

// TestBufBinWriterBufferSizes tests different buffer sizes
func TestBufBinWriterBufferSizes(t *testing.T) {
	root := GetTestDir("test_buf_sizes")
	_ = os.MkdirAll(root, 0755)
	defer CleanupTestDir(root)

	testDocs := CreateTestDocs(50)
	bufferSizes := []int{4 * 1024, 16 * 1024, 64 * 1024, 256 * 1024}

	for _, bufSize := range bufferSizes {
		t.Run(fmt.Sprintf("BufferSize_%d", bufSize), func(t *testing.T) {
			testFile := filepath.Join(root, fmt.Sprintf("test_%d.bin", bufSize))
			writer := NewBufBinWriter(testFile, NONE, bufSize)
			err := writer.Open()
			if err != nil {
				t.Fatalf("Open failed: %v", err)
			}

			for _, doc := range testDocs {
				_, err := writer.Write(doc)
				if err != nil {
					t.Fatalf("Write failed: %v", err)
				}
			}

			err = writer.Close()
			if err != nil {
				t.Fatalf("Close failed: %v", err)
			}

			// Verify file is readable
			reader, err := NewBinReader(testFile, NONE)
			if err != nil {
				t.Fatalf("NewBinReader failed: %v", err)
			}
			defer reader.Close()

			count := reader.Count(&CountOption{Offset: 0, End: -1, WorkerCount: 1, SkipError: false})
			if count != int64(len(testDocs)) {
				t.Errorf("Document count mismatch: expected=%d, actual=%d", len(testDocs), count)
			}
		})
	}
}

// TestBufBinWriterLargeFile tests writing large number of documents
func TestBufBinWriterLargeFile(t *testing.T) {
	root := GetTestDir("test_buf_large")
	_ = os.MkdirAll(root, 0755)
	defer CleanupTestDir(root)

	testFile := filepath.Join(root, "large_file.bin")
	testDocs := CreateTestDocs(1000)

	writer := NewBufBinWriter(testFile, NONE, 64*1024)
	err := writer.Open()
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer func() {
		_ = writer.Close()
	}()

	for _, doc := range testDocs {
		_, err := writer.Write(doc)
		if err != nil {
			t.Fatalf("Write failed: %v", err)
		}
	}

	// Ensure all data is flushed before closing
	if bufWriter, ok := writer.(*bufBinWriter); ok {
		err = bufWriter.Flush()
		if err != nil {
			t.Fatalf("Flush failed: %v", err)
		}
	}

	err = writer.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Verify all documents were written
	reader, err := NewBinReader(testFile, NONE)
	if err != nil {
		t.Fatalf("NewBinReader failed: %v", err)
	}
	defer reader.Close()

	count := reader.Count(&CountOption{Offset: 0, End: -1, WorkerCount: 1, SkipError: false})
	if count != int64(len(testDocs)) {
		t.Errorf("Document count mismatch: expected=%d, actual=%d", len(testDocs), count)
	}
}
