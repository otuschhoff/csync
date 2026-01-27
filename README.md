# csync - Fast Recursive Directory Synchronization

A high-performance Go library for recursive directory tree synchronization with parallel processing, work stealing, and extensible callbacks.

## Objectives

The csync package aims to provide:

1. **Fast Parallel Synchronization** - Synchronize large directory trees efficiently using configurable worker pools
2. **Work Stealing Load Balancing** - Distribute work dynamically among workers for optimal parallelism
3. **Comprehensive File Handling** - Support regular files, directories, and symlinks with automatic conflict resolution
4. **Extensible Monitoring** - Track synchronization operations through optional callbacks for custom logging and monitoring
5. **Read-Only Mode** - Perform dry-run validations or audits without modifying the destination
6. **Automatic Cleanup** - Remove destination-only files and directories to maintain synchronization state
7. **Race-Safe Operations** - Built-in thread-safety for concurrent operations (verified with Go race detector)

## Key Features

- **Parallel Directory Walking** - Configurable worker pool for concurrent directory processing
- **Work Stealing Algorithm** - Idle workers steal tasks from busy workers to maintain optimal load distribution
- **Callback Hooks** - Monitor operations like file copies, directory creation, file removal, and more
- **Read-Only Mode** - Validate synchronization without making changes to the destination
- **Symlink Support** - Properly handles symbolic links with target matching
- **Automatic Cleanup** - Removes files/directories that exist only in the destination
- **Context Cancellation** - Support for graceful shutdown with context cancellation
- **Race Condition Free** - Passes Go's race detector on all operations

## Installation

```bash
go get csync
```

## Usage

### Basic Synchronization

Synchronize a source directory tree to a destination:

```go
package main

import (
	"log"
	"csync"
)

func main() {
	srcDir := "/source/path"
	dstDir := "/dest/path"
	
	// Create synchronizer with 4 worker threads
	sync := csync.NewSynchronizer(srcDir, dstDir, 4, false, csync.Callbacks{})
	
	// Start synchronization
	if err := sync.Run(); err != nil {
		log.Fatalf("Synchronization failed: %v", err)
	}
	
	log.Println("Synchronization complete")
}
```

### With Monitoring Callbacks

Track synchronization operations:

```go
callbacks := csync.Callbacks{
	OnCopy: func(srcPath, dstPath string, size int64, err error) {
		if err != nil {
			log.Printf("Error copying %s: %v", srcPath, err)
		} else {
			log.Printf("Copied %s (%d bytes)", srcPath, size)
		}
	},
	OnMkdir: func(path string, mode os.FileMode, err error) {
		if err == nil {
			log.Printf("Created directory: %s", path)
		}
	},
	OnRemoveAll: func(path string, err error) {
		if err == nil {
			log.Printf("Removed: %s", path)
		}
	},
}

sync := csync.NewSynchronizer(srcDir, dstDir, 4, false, callbacks)
if err := sync.Run(); err != nil {
	log.Fatalf("Synchronization failed: %v", err)
}
```

### Read-Only Mode (Dry-Run)

Validate synchronization without making changes:

```go
// readOnly = true prevents any modifications
sync := csync.NewSynchronizer(srcDir, dstDir, 4, true, callbacks)
if err := sync.Run(); err != nil {
	log.Fatalf("Validation failed: %v", err)
}
log.Println("Validation complete - no changes made")
```

### Available Callbacks

The `Callbacks` struct provides optional hooks for:

- **OnLstat** - Called after examining a file/directory
- **OnReadDir** - Called after reading a directory
- **OnCopy** - Called when copying a file
- **OnMkdir** - Called when creating a directory
- **OnUnlink** - Called when removing a file or symlink
- **OnRemoveAll** - Called when removing a directory tree
- **OnSymlink** - Called when creating a symlink
- **OnChmod** - Called when changing permissions (reserved)
- **OnChown** - Called when changing ownership (reserved)
- **OnChtimes** - Called when changing modification times (reserved)

### Custom Logger

By default, csync logs errors to the standard `log` package. You can provide a custom logger implementing the `Logger` interface to integrate with your logging system (e.g., structured logging):

```go
package main

import (
	"fmt"
	"log/slog"
	"csync"
)

// Adapter to use slog with csync
type slogAdapter struct {
	logger *slog.Logger
}

func (sa *slogAdapter) Printf(format string, v ...interface{}) {
	sa.logger.Warn("csync", "message", fmt.Sprintf(format, v...))
}

func main() {
	srcDir := "/source"
	dstDir := "/destination"
	
	logger := &slogAdapter{logger: slog.Default()}
	
	// Use custom logger
	sync := csync.NewSynchronizerWithLogger(srcDir, dstDir, 4, false, csync.Callbacks{}, logger)
	if err := sync.Run(); err != nil {
		slog.Error("sync failed", "err", err)
	}
}
```

The `Logger` interface is simple:
```go
type Logger interface {
	Printf(format string, v ...interface{})
}
```

## Synchronization Behavior

### What Gets Synchronized

1. **Directories** - Created in destination if missing
2. **Files** - Copied if missing or if size/modification time differs
3. **Symlinks** - Created with matching targets
4. **Permissions** - Directory permissions are preserved

### Conflict Resolution

- **File vs Directory** - The destination item is removed and replaced
- **Type Mismatch** - Destination items of wrong type are automatically removed
- **Modified Files** - Overwritten if source size or mtime differs
- **Destination-Only Entries** - Automatically removed to match source

### Special Handling

- The `.snapshot` directory is skipped (never synchronized)
- Empty directories are created but not specially handled
- Modification times of copied files are not updated
- Only size and modification time are compared for files

## Worker Pool and Work Stealing

The synchronizer uses a configurable number of worker threads:

```go
// Single worker (sequential processing)
sync := csync.NewSynchronizer(src, dst, 1, false, callbacks)

// 4 workers with work stealing
sync := csync.NewSynchronizer(src, dst, 4, false, callbacks)

// 8 workers for large directory trees
sync := csync.NewSynchronizer(src, dst, 8, false, callbacks)
```

Work stealing ensures that:
- Workers don't sit idle when others have work
- Load is automatically balanced among workers
- Large directory subtrees can be processed in parallel

## Testing

Run all tests:

```bash
go test -v
```

Run with race detector to verify thread safety:

```bash
go test -v -race
```

The test suite includes:

- **TestBasicSync** - Basic file and directory synchronization
- **TestSymlinkSync** - Symlink handling
- **TestDeleteSync** - Deletion of destination-only files
- **TestReadOnly** - Read-only mode verification
- **TestLargeParallelSync** - Parallel processing with 500+ files
- **TestLargeParallelSync2** - Additional parallel test with 256 files
- **TestParallelWithMixedContent** - Mixed file type handling
- **TestDirectoryStructureIntegrity** - Nested directory verification
- **TestConsistency** - Idempotency verification

All tests pass with Go's race detector enabled, ensuring thread safety.

## Performance Considerations

1. **Worker Count** - Use 4-8 workers for most workloads, scale up for very large trees
2. **File Size** - Large files are copied efficiently using buffered I/O
3. **Directory Depth** - Deep directory structures benefit from parallel processing
4. **Network Filesystems** - May have different performance characteristics; test for your use case
5. **Callbacks** - Keep callback implementations fast to avoid bottlenecks

## Block-Based Hashing

For file integrity verification, csync supports optional block-based hashing during copy operations. Files are processed in 4K blocks, with each block hashed using a selectable algorithm.

### Supported Algorithms

- **`HashAlgoMD5`** - MD5 (128 bits, 16 bytes) - Cryptographic, widely compatible
- **`HashAlgoSHA256`** - SHA-256 (256 bits, 32 bytes) - Cryptographic, secure
- **`HashAlgoSHA512`** - SHA-512 (512 bits, 64 bytes) - Cryptographic, highest security
- **`HashAlgoXXHash`** - xxHash (64 bits, 8 bytes) - Non-cryptographic, extremely fast
- **`HashAlgoNone`** - Disable hashing (default)

**Performance Considerations:**
- xxHash is 10-20x faster than MD5 and suitable for integrity checking in non-adversarial scenarios
- Cryptographic algorithms (MD5, SHA256, SHA512) are resistant to collision attacks but slower
- Choose based on your security requirements and performance constraints

### Using Block Hashing

Hashes can be received via callback interface, channel, or both simultaneously:

#### Callback-Based Delivery

Implement the `BlockHasher` interface to receive hash values:

```go
type BlockHasher interface {
    HashBlock(blockID uint64, hash []byte) error
}
```

Example: Callback-based hash collection:

```go
type hashCollector struct {
    hashes map[uint64][]byte
    mu     sync.Mutex
}

func (hc *hashCollector) HashBlock(blockID uint64, hash []byte) error {
    hc.mu.Lock()
    defer hc.mu.Unlock()
    
    // blockID is 0-indexed and incremented independently
    hashCopy := make([]byte, len(hash))
    copy(hashCopy, hash)
    hc.hashes[blockID] = hashCopy
    return nil
}

// Use callback-based delivery
sync := NewSynchronizer(srcDir, dstDir, 4, false, Callbacks{})
collector := &hashCollector{hashes: make(map[uint64][]byte)}
sync.copyFileWithHash(srcFile, dstFile, HashAlgoSHA256, collector)
```

#### Channel-Based Delivery

Receive hashes via a channel for decoupled processing:

```go
// Create buffered channel for block hashes
hashChan := make(chan BlockHash, 100)
defer close(hashChan)

// Copy with channel-based delivery
sync := NewSynchronizer(srcDir, dstDir, 4, false, Callbacks{})
go func() {
    err := sync.copyFileWithHashChannel(srcFile, dstFile, HashAlgoSHA256, nil, hashChan)
    if err != nil {
        log.Fatalf("copy failed: %v", err)
    }
}()

// Receive hashes from channel
for blockHash := range hashChan {
    fmt.Printf("Block %d: %x\n", blockHash.BlockID, blockHash.Hash)
}
```

#### Dual Delivery (Callback + Channel)

Receive hashes via both mechanisms simultaneously:

```go
hashChan := make(chan BlockHash, 100)
defer close(hashChan)

collector := &hashCollector{hashes: make(map[uint64][]byte)}

// Both callback and channel will receive all hashes
sync := NewSynchronizer(srcDir, dstDir, 4, false, Callbacks{})
err := sync.copyFileWithHashChannel(srcFile, dstFile, HashAlgoSHA256, collector, hashChan)

// Process from either or both sources
fmt.Printf("Callback collected: %d blocks\n", len(collector.hashes))
```

### Key Points

- Both producer (copy) and consumer maintain independent block ID counters starting from 0
- Block IDs are sequential: 0, 1, 2, ...
- Each block is up to 4096 bytes; partial blocks at file end are hashed as-is
- Hash bytes should be copied by the consumer; the caller does not retain references
- Channel sends are **non-blocking**; if the channel is full, sends are skipped
- Callback errors are **blocking** and will abort the copy
- Hashing adds minimal overhead; use `HashAlgoNone` for the fastest copy

## API Reference

### Types

- **`Synchronizer`** - Main synchronization engine
- **`Callbacks`** - Optional operation hooks
- **`Logger`** - Interface for custom logging
- **`BlockHasher`** - Interface for consuming block hashes
- **`HashAlgo`** - Hash algorithm selector (MD5, SHA256, SHA512, None)
- **`BlockHash`** - Struct containing block ID and hash bytes

### Functions

- **`NewSynchronizer(srcRoot, dstRoot string, numWorkers int, readOnly bool, callbacks Callbacks) *Synchronizer`**
  - Create a new synchronizer instance with default logger
  - `numWorkers`: Number of parallel workers (min 1)
  - `readOnly`: When true, prevents modifications to destination
  - `callbacks`: Optional operation hooks (can be empty)

- **`NewSynchronizerWithLogger(srcRoot, dstRoot string, numWorkers int, readOnly bool, callbacks Callbacks, logger Logger) *Synchronizer`**
  - Create a new synchronizer with custom logger
  - `logger`: Custom Logger implementation (nil uses default)

### Methods

- **`Run() error`** - Start synchronization (blocks until complete)
- **`Stop()`** - Cancel synchronization gracefully
- **`copyFileWithHash(srcPath, dstPath string, algo HashAlgo, hasher BlockHasher) error`** - Copy file with callback-based block hashing
  - `algo`: Hash algorithm to use (HashAlgoNone to disable)
  - `hasher`: Callback to receive block hashes (can be nil if algo is HashAlgoNone)
- **`copyFileWithHashChannel(srcPath, dstPath string, algo HashAlgo, hasher BlockHasher, hashChan chan<- BlockHash) error`** - Copy file with callback and/or channel-based hashing
  - `algo`: Hash algorithm to use (HashAlgoNone to disable)
  - `hasher`: Optional callback to receive block hashes (can be nil)
  - `hashChan`: Optional channel to receive BlockHash values (can be nil, non-blocking sends)

## Inode Attribute Syncing

During synchronization, csync automatically syncs inode attributes from source to destination:

### Synced Attributes

- **Permissions** - File and directory mode/permissions are synced via `chmod`
- **Modification/Access Times** - File timestamps are synced via `chtimes`
- **Ownership** - UID/GID are synced via `chown` (requires appropriate permissions, typically root)
- **Symlink Targets** - Symlink targets are verified to match; mismatches trigger recreation

### How It Works

1. **Directories**: Permissions and times are synced after creation and on every pass
2. **Files**: Permissions and times are synced after copy
3. **Symlinks**: Targets are verified on each pass; mismatched targets cause the symlink to be removed and recreated
4. **Errors**: Inode syncing errors (permissions, chown failures) are logged as warnings/debug but do not abort the sync

### Special Cases

- **Ownership (chown)**: Non-root users typically cannot change ownership of files they don't own. Errors are logged at DEBUG level and ignored.
- **Permissions**: Always synced when possible; failures are logged at WARN level.
- **Times**: Synced with nanosecond precision when supported by the filesystem.

## Limitations

- Does not copy extended attributes (xattr)
- Hard links are treated as separate files
- ACLs are not synchronized
- SELinux contexts are not synchronized
- File capabilities are not synchronized

## License

MIT License - see LICENSE file for details

## Contributing

Contributions are welcome. Please ensure:
- All tests pass: `go test -v`
- No race conditions: `go test -v -race`
- Code is properly documented with GoDoc comments

## Examples

### Monitor All Operations

```go
callbacks := csync.Callbacks{
	OnReadDir: func(path string, entries []os.DirEntry, err error) {
		if err == nil {
			log.Printf("Reading %s (%d entries)", path, len(entries))
		}
	},
	OnCopy: func(srcPath, dstPath string, size int64, err error) {
		if err == nil {
			log.Printf("Copied: %s", srcPath)
		}
	},
	OnMkdir: func(path string, mode os.FileMode, err error) {
		if err == nil {
			log.Printf("Created: %s", path)
		}
	},
	OnRemoveAll: func(path string, err error) {
		if err == nil {
			log.Printf("Removed: %s", path)
		}
	},
	OnSymlink: func(linkPath, target string, err error) {
		if err == nil {
			log.Printf("Created symlink: %s -> %s", linkPath, target)
		}
	},
}

sync := csync.NewSynchronizer(src, dst, 4, false, callbacks)
sync.Run()
```

### Concurrent Synchronization

```go
import "sync"

var wg sync.WaitGroup

// Synchronize multiple directory pairs concurrently
pairs := [][2]string{
	{"/src1", "/dst1"},
	{"/src2", "/dst2"},
	{"/src3", "/dst3"},
}

for _, pair := range pairs {
	wg.Add(1)
	go func(src, dst string) {
		defer wg.Done()
		sync := csync.NewSynchronizer(src, dst, 4, false, csync.Callbacks{})
		sync.Run()
	}(pair[0], pair[1])
}

wg.Wait()
```
