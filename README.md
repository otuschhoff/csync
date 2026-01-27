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

// Logger interface that csync expects
// type Logger interface {
//     Printf(format string, v ...interface{})
// }

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
	callbacks := csync.Callbacks{} // optional callbacks
	
	logger := &slogAdapter{logger: slog.Default()}
	sync := csync.NewSynchronizerWithLogger(srcDir, dstDir, 4, false, callbacks, logger)
	
	if err := sync.Run(); err != nil {
		log.Fatalf("Synchronization failed: %v", err)
	}
}
```

The `Logger` interface is simple - you only need to implement `Printf(format string, v ...interface{})`. This allows integration with any logging framework while keeping csync itself dependency-free.

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

Block hashing is delivered via callback interface. Implement the `BlockHasher` interface to receive hash values:

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
	- **OnIgnore** - Called to determine if a file/directory should be skipped (not synced)
    return nil
	### Ignoring Files and Directories
}
	You can dynamically exclude files and directories from synchronization using the `OnIgnore` callback:

	```go
	package main
// Use callback-based delivery
	import (
		"os"
		"path/filepath"
		"strings"
		"csync"
	)
sync := NewSynchronizer(srcDir, dstDir, 4, false, Callbacks{})
	func main() {
		srcDir := "/source"
		dstDir := "/destination"

		callbacks := csync.Callbacks{
			OnIgnore: func(name string, path string, isDir bool, fileInfo os.FileInfo, err error) bool {
				if err != nil {
					return false // Process on error
				}
collector := &hashCollector{hashes: make(map[uint64][]byte)}
				// Ignore .snapshot directories
				if isDir && name == ".snapshot" {
					return true
				}
sync.copyFileWithHash(srcFile, dstFile, HashAlgoSHA256, collector)
				// Ignore hidden files (starting with .)
				if strings.HasPrefix(name, ".") && name != ".." {
					return true
				}
```
				// Ignore temporary files
				if strings.HasSuffix(name, ".tmp") {
					return true
				}

				return false // Process this entry
			},
		}
### Key Points
		sync := csync.NewSynchronizer(srcDir, dstDir, 4, false, callbacks)
		if err := sync.Run(); err != nil {
			panic(err)
		}
	}
	```

	The `OnIgnore` callback receives:
	- `name` - The basename of the entry
	- `path` - The absolute path to the entry
	- `isDir` - Whether the entry is a directory
	- `fileInfo` - File information from lstat (contains permissions, size, times, etc.)
	- `err` - Any error from lstat (usually nil)
- Producer and consumer maintain independent block ID counters starting from 0
	Return `true` to skip the entry, `false` to process it.
- Block IDs are sequential: 0, 1, 2, ...
	### Custom Logger
- Each block is up to 4096 bytes; partial blocks at file end are hashed as-is
- Hash bytes should be copied by the consumer; the caller does not retain references
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

## API Reference

### Constants

#### Hash Algorithms

```go
const (
	HashAlgoNone   HashAlgo = ""        // Disable hashing
	HashAlgoMD5    HashAlgo = "md5"     // MD5 (128 bits, 16 bytes)
	HashAlgoSHA256 HashAlgo = "sha256"  // SHA-256 (256 bits, 32 bytes)
	HashAlgoSHA512 HashAlgo = "sha512"  // SHA-512 (512 bits, 64 bytes)
	HashAlgoXXHash HashAlgo = "xxhash"  // xxHash (64 bits, 8 bytes, very fast)
)
```

#### Package Version

```go
const Version = "0.1.0"  // Current package version
```

### Types

#### Synchronizer

The main synchronizer for directory tree synchronization.

```go
type Synchronizer struct {
	// Contains private fields
}
```

**Methods:**

- `Run() error` - Start synchronization and block until complete
- `Stop()` - Cancel synchronization gracefully
- `copyFileWithHash(srcPath, dstPath string, algo HashAlgo, hasher BlockHasher) error` - Copy a file with optional block hashing using callback

#### Callbacks

Struct defining optional hooks for monitoring synchronization operations.

```go
type Callbacks struct {
	OnLstat        func(path string, isDir bool, fileInfo os.FileInfo, err error)
	OnReadDir      func(path string, entries []os.DirEntry, err error)
	OnCopy         func(srcPath, dstPath string, size int64, err error)
	OnMkdir        func(path string, mode os.FileMode, err error)
	OnUnlink       func(path string, err error)
	OnRmdir        func(path string, err error)
	OnRemoveAll    func(path string, err error)
	OnSymlink      func(linkPath, target string, err error)
	OnChmod        func(path string, mode os.FileMode, err error)
	OnChown        func(path string, uid, gid int, err error)
	OnChtimes      func(path string, err error)
	OnIgnore       func(name string, path string, isDir bool, fileInfo os.FileInfo, err error) bool
}
```

All callback fields are optional; zero values are ignored.

#### Logger

Interface for custom logging implementations.

```go
type Logger interface {
	Printf(format string, v ...interface{})
}
```

Implementations should safely format and log messages. The default implementation wraps the standard `log` package.

#### BlockHasher

Interface for receiving block hashes during file copy operations.

```go
type BlockHasher interface {
	HashBlock(blockID uint64, hash []byte) error
}
```

- `blockID` is 0-indexed and incremented by the copy operation
- `hash` contains the hash bytes; implementations should copy if they need to retain the value
- Returning an error will abort the copy operation

#### BlockHash

Represents a single block hash during file copy.

```go
type BlockHash struct {
	BlockID uint64  // 0-indexed block number
	Hash    []byte  // Hash bytes (length depends on algorithm)
}
```

#### HashAlgo

String type specifying the hash algorithm to use.

```go
type HashAlgo string
```

Valid values: `HashAlgoNone`, `HashAlgoMD5`, `HashAlgoSHA256`, `HashAlgoSHA512`, `HashAlgoXXHash`

### Functions

#### NewSynchronizer

Create a new synchronizer with default logging.

```go
func NewSynchronizer(srcRoot, dstRoot string, numWorkers int, readOnly bool, callbacks Callbacks) *Synchronizer
```

**Parameters:**
- `srcRoot` - Source directory path
- `dstRoot` - Destination directory path
- `numWorkers` - Number of parallel workers (minimum 1; values ≤ 0 default to 1)
- `readOnly` - If true, no modifications are made to the destination
- `callbacks` - Optional callbacks for monitoring operations

**Returns:** New Synchronizer instance

#### NewSynchronizerWithLogger

Create a new synchronizer with a custom logger.

```go
func NewSynchronizerWithLogger(srcRoot, dstRoot string, numWorkers int, readOnly bool, callbacks Callbacks, logger Logger) *Synchronizer
```

**Parameters:**
- `srcRoot` - Source directory path
- `dstRoot` - Destination directory path
- `numWorkers` - Number of parallel workers (minimum 1; values ≤ 0 default to 1)
- `readOnly` - If true, no modifications are made to the destination
- `callbacks` - Optional callbacks for monitoring operations
- `logger` - Custom Logger implementation; if nil, default logger is used

**Returns:** New Synchronizer instance

### Synchronizer Methods

#### Run

Start the synchronization process.

```go
func (s *Synchronizer) Run() error
```

Initializes a worker pool and processes the directory tree in parallel. This call blocks until all workers complete.

**Returns:** Error if synchronization fails, nil on success

#### Stop

Cancel the synchronization process.

```go
func (s *Synchronizer) Stop()
```

Gracefully cancels the synchronization, causing workers to exit.

#### copyFileWithHash

Copy a file with optional callback-based block hashing.

```go
func (s *Synchronizer) copyFileWithHash(srcPath, dstPath string, algo HashAlgo, hasher BlockHasher) error
```

**Parameters:**
- `srcPath` - Source file path
- `dstPath` - Destination file path
- `algo` - Hash algorithm (HashAlgoNone to disable)
- `hasher` - BlockHasher implementation for callback-based delivery; can be nil

**Returns:** Error if copy fails or hasher returns error, nil on success

### Behavior Details

#### Read-Only Mode

When `readOnly` is true:
- No files, directories, or symlinks are created
- No modifications are made to the destination
- Callbacks are still invoked to allow monitoring of what *would* be done

#### Work Stealing Load Balancing

The synchronizer uses a work-stealing algorithm:
- Each worker maintains a queue of directory branches
- When a worker's queue is empty, it attempts to steal work from other workers
- A worker steals items from other workers with >1 item in their queue
- This ensures idle workers don't block when others have work available

#### Inode Attribute Synchronization

For all files and directories:
- **Permissions** - File mode bits are synced via chmod
- **Times** - Modification and access times are synced with nanosecond precision
- **Ownership** - UID/GID are synced via chown (requires appropriate permissions)

For symlinks:
- **Targets** - Symlink targets are verified; mismatches cause removal and recreation
- **Other attributes** - Symlink targets are compared; other symlink attributes follow system conventions

Inode sync errors are logged but do not abort the synchronization.

#### Block Hashing

Files are processed in 4K blocks:
- Partial blocks (at file end) are hashed as-is
- Both producer and consumer maintain independent block ID counters from 0
- Hash algorithms produce different output sizes:
  - MD5: 16 bytes
  - SHA256: 32 bytes
  - SHA512: 64 bytes
  - xxHash: 8 bytes
- For xxHash, the 64-bit result is returned as 8 bytes
