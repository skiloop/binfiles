# BinFiles

A file format to store large number of small files efficiently.

## Features

- **Multiple Compression Support**: GZIP, Brotli, BZIP2, LZ4, XZ
- **Memory Pool Optimization**: Reduces GC pressure and improves performance
- **Concurrent Processing**: Multi-worker support for large file operations
- **File Locking**: Safe concurrent access to files
- **Flexible API**: Both programmatic and command-line interfaces

## Quick Start

```bash
# Install
go get github.com/skiloop/binfiles

# Package files
binutil package output.bin /path/to/files

# List contents
binutil list input.bin

# Repack with compression
binutil repack source.bin target.bin --target-compress=gzip
```

## TODO

### Feature Enhancements
- [ ] add tests (improve test coverage)
- [ ] add file buffer (file buffer optimization)
- [ ] add doc index (document indexing functionality)
- [ ] Implement file header and version control
- [ ] Support incremental updates
- [ ] Add batch I/O operations optimization

### Performance Optimization
- [ ] Fix concurrent counting bug (TODO in binreader.go)
- [ ] Optimize worker pool management
- [ ] Improve file locking mechanism
- [ ] Add read-ahead buffer
- [ ] Optimize file seek operations
- [ ] Implement index compression

### Code Quality
- [ ] Clean up commented code
- [ ] Improve error handling mechanism
- [ ] Refactor interface design, reduce code duplication
- [ ] Unify compressor interfaces
- [ ] Add more linter rules
- [ ] Integrate golangci-lint

### Testing and Documentation
- [ ] Add integration tests
- [ ] Large file processing tests
- [ ] Concurrency safety tests
- [ ] Error recovery tests
- [ ] Memory leak tests
- [ ] Complete API documentation
- [ ] Add usage examples
- [ ] Provide performance benchmarks

### Tools and Monitoring
- [ ] Improve command-line tool error messages
- [ ] Add progress bar display
- [ ] Add performance metrics monitoring
- [ ] Compression ratio statistics
- [ ] Processing speed monitoring
- [ ] Memory usage tracking

## Known Issues
- [ ] doc missing when merge
- [ ] Concurrent counting mismatch issue
- [ ] Incomplete file corruption handling