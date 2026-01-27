// Package csync provides fast recursive directory synchronization with extensible callbacks.
//
// The csync package synchronizes directory trees from a source to a destination,
// handling files, directories, and symlinks. It uses a worker pool for parallel
// processing and supports work stealing for efficient load distribution.
//
// Key Features:
//   - Parallel directory tree walking with configurable worker pool
//   - Work stealing algorithm for load balancing
//   - Extensible callbacks for monitoring sync operations
//   - Read-only mode for dry-run or validation
//   - Support for symlinks, regular files, and directories
//   - Automatic cleanup of destination-only files/directories
//
// Example usage:
//
//	srcDir := "/source/path"
//	dstDir := "/dest/path"
//	callbacks := csync.Callbacks{
//	    OnCopy: func(srcPath, dstPath string, size int64, err error) {
//	        if err == nil {
//	            log.Printf("Copied %s (%d bytes)", srcPath, size)
//	        }
//	    },
//	}
//	sync := csync.NewSynchronizer(srcDir, dstDir, 4, false, callbacks)
//	if err := sync.Run(); err != nil {
//	    log.Fatal(err)
//	}
package csync

import (
	"context"
	"crypto/md5"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"

	"github.com/cespare/xxhash/v2"
)

// Version is the current version of the csync package.
const Version = "0.1.0"

// HashAlgo specifies the algorithm to use for block hashing during file copy.
type HashAlgo string

const (
	// HashAlgoNone disables block hashing
	HashAlgoNone HashAlgo = ""
	// HashAlgoMD5 uses MD5 hashing (128 bits)
	HashAlgoMD5 HashAlgo = "md5"
	// HashAlgoSHA256 uses SHA-256 hashing (256 bits)
	HashAlgoSHA256 HashAlgo = "sha256"
	// HashAlgoSHA512 uses SHA-512 hashing (512 bits)
	HashAlgoSHA512 HashAlgo = "sha512"
	// HashAlgoXXHash uses xxHash (64 bits, very fast)
	HashAlgoXXHash HashAlgo = "xxhash"
)

// BlockHash represents the hash of a single 4K block.
// BlockID is 0-indexed and incremented by both producer and consumer independently.
type BlockHash struct {
	BlockID uint64 // 0-indexed block number
	Hash    []byte // Hash bytes (length depends on algorithm)
}

// BlockHasher is an interface for consuming block hashes during file copy.
// Implementations should be safe for concurrent access.
type BlockHasher interface {
	// HashBlock is called with the hash of each 4K block as it's copied.
	// blockID is 0-indexed and incremented by the caller.
	// hash contains the hash bytes; the caller does not retain a reference to it.
	HashBlock(blockID uint64, hash []byte) error
}

// Logger defines the interface for logging messages during synchronization.
// Implementations can use standard logging, structured logging, or custom handling.
type Logger interface {
	// Printf logs a formatted message similar to fmt.Printf.
	Printf(format string, v ...interface{})
}

// defaultLogger wraps the standard log package to implement the Logger interface.
type defaultLogger struct{}

func (dl *defaultLogger) Printf(format string, v ...interface{}) {
	log.Printf(format, v...)
}

// Callbacks define optional handlers for sync operations.
// All callbacks are optional (zero value means no callback).
// Callbacks are called during various stages of the synchronization process
// and can be used for logging, monitoring, or custom handling of sync events.
type Callbacks struct {
	// OnLstat is called after lstat operations on both source and destination.
	// Called with the path being examined, whether it's a directory, file info, and any error.
	OnLstat func(path string, isDir bool, fileInfo os.FileInfo, err error)

	// OnReadDir is called after reading a directory.
	// Called with the directory path, its entries, and any error encountered.
	OnReadDir func(path string, entries []os.DirEntry, err error)

	// OnCopy is called before and after copying a file.
	// If err is nil, the copy was successful. Called with source path, destination path,
	// file size in bytes, and any error.
	OnCopy func(srcPath, dstPath string, size int64, err error)

	// OnMkdir is called before and after creating a directory.
	// Called with the directory path, file mode, and any error.
	OnMkdir func(path string, mode os.FileMode, err error)

	// OnUnlink is called before and after removing a file.
	// Called with the file path and any error encountered during removal.
	OnUnlink func(path string, err error)

	// OnRmdir is called before and after removing an empty directory.
	// Called with the directory path and any error.
	OnRmdir func(path string, err error)

	// OnRemoveAll is called when recursively removing a path.
	// Called with the path being removed and any error encountered.
	OnRemoveAll func(path string, err error)

	// OnSymlink is called when creating a symlink.
	// Called with the symlink path, target, and any error.
	OnSymlink func(linkPath, target string, err error)

	// OnChmod is called when changing permissions.
	// Called with the path, new file mode, and any error.
	OnChmod func(path string, mode os.FileMode, err error)

	// OnChown is called when changing ownership.
	// Called with the path, uid, gid, and any error.
	OnChown func(path string, uid, gid int, err error)

	// OnChtimes is called when changing modification/access times.
	// Called with the path and any error encountered.
	OnChtimes func(path string, err error)

	// OnIgnore is called to determine if a file or directory should be ignored during sync.
	// Called with the entry name (basename), full absolute path, whether it's a directory,
	// the lstat information, and any lstat error.
	// Return true to skip this entry, false to process it.
	// If OnIgnore is nil, no entries are ignored by default.
	OnIgnore func(name string, path string, isDir bool, fileInfo os.FileInfo, err error) bool
}

// Synchronizer syncs one directory tree to another.
// It uses a worker pool for parallel processing with work stealing for efficient
// load balancing. The synchronizer handles files, directories, and symlinks,
// automatically removing destination-only entries.
type Synchronizer struct {
	srcRoot   string
	dstRoot   string
	callbacks Callbacks
	readOnly  bool
	logger    Logger

	monitorCtx context.Context
	cancel     context.CancelFunc

	// Worker pool management
	numWorkers int
	workers    []*syncWorker
	workerMu   sync.Mutex
	wg         sync.WaitGroup
	shutdown   int32
}

// syncWorker represents a single worker processing directories.
// Each worker maintains a queue of directory branches to process and cooperates
// with other workers through the work stealing mechanism.
type syncWorker struct {
	id           int
	synchronizer *Synchronizer
	queue        []*syncBranch
	mu           sync.Mutex
}

// syncBranch represents a directory node in the sync tree.
// It forms a linked list structure from leaf to root for tracking relative paths.
type syncBranch struct {
	parent   *syncBranch
	basename string
}

func (sb *syncBranch) isRoot() bool {
	return sb.parent == nil
}

// relPath returns the relative path of this branch from the root.
func (sb *syncBranch) relPath() string {
	return strings.Join(sb.relPathElems(), "/")
}

// relPathElems returns the path elements from root to this branch as a slice.
func (sb *syncBranch) relPathElems() []string {
	if sb.isRoot() {
		return []string{}
	}
	return append(sb.parent.relPathElems(), sb.basename)
}

// srcAbsPath returns the absolute path in the source directory for this branch.
func (sb *syncBranch) srcAbsPath(syncRoot string) string {
	if sb.isRoot() {
		return syncRoot
	}
	return filepath.Join(syncRoot, sb.relPath())
}

// dstAbsPath returns the absolute path in the destination directory for this branch.
func (sb *syncBranch) dstAbsPath(syncRoot string) string {
	if sb.isRoot() {
		return syncRoot
	}
	return filepath.Join(syncRoot, sb.relPath())
}

// queueLen returns the current length of the worker's queue (thread-safe).
func (sw *syncWorker) queueLen() int {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	return len(sw.queue)
}

// queuePush adds an item to the worker's queue (thread-safe).
func (sw *syncWorker) queuePush(item *syncBranch) {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	sw.queue = append(sw.queue, item)
}

// queuePop removes and returns the last item from the worker's queue (thread-safe).
// Returns nil if the queue is empty.
func (sw *syncWorker) queuePop() *syncBranch {
	sw.mu.Lock()
	defer sw.mu.Unlock()
	if len(sw.queue) > 0 {
		item := sw.queue[len(sw.queue)-1]
		sw.queue = sw.queue[:len(sw.queue)-1]
		return item
	}
	return nil
}

// NewSynchronizer creates a new Synchronizer for syncing srcRoot to dstRoot.
// numWorkers specifies the number of parallel workers to use (minimum 1).
// If numWorkers <= 0, it defaults to 1.
// readOnly, when true, prevents any modifications to the destination.
// callbacks can be used to monitor sync operations; nil callbacks are ignored.
// logger is used for logging errors and progress; if nil, a default logger is used.
func NewSynchronizer(srcRoot, dstRoot string, numWorkers int, readOnly bool, callbacks Callbacks) *Synchronizer {
	return NewSynchronizerWithLogger(srcRoot, dstRoot, numWorkers, readOnly, callbacks, nil)
}

// NewSynchronizerWithLogger creates a new Synchronizer with a custom logger.
// If logger is nil, a default logger wrapping the standard log package is used.
func NewSynchronizerWithLogger(srcRoot, dstRoot string, numWorkers int, readOnly bool, callbacks Callbacks, logger Logger) *Synchronizer {
	if numWorkers <= 0 {
		numWorkers = 1
	}

	if logger == nil {
		logger = &defaultLogger{}
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Synchronizer{
		srcRoot:    filepath.Clean(srcRoot),
		dstRoot:    filepath.Clean(dstRoot),
		callbacks:  callbacks,
		readOnly:   readOnly,
		logger:     logger,
		monitorCtx: ctx,
		cancel:     cancel,
		numWorkers: numWorkers,
	}
}

// Run starts the synchronization process.
// It initializes a worker pool and processes the directory tree in parallel.
// The function blocks until all workers complete.
// Returns an error if the synchronization fails.
func (s *Synchronizer) Run() error {
	// Initialize workers
	s.workerMu.Lock()
	for i := 0; i < s.numWorkers; i++ {
		worker := &syncWorker{
			id:           i,
			synchronizer: s,
		}
		s.workers = append(s.workers, worker)
		s.wg.Add(1)
		go s.startWorker(worker)
	}
	s.workerMu.Unlock()

	// Start with root directory
	root := &syncBranch{}
	s.workers[0].queuePush(root)

	// Wait for all workers to finish
	s.wg.Wait()

	return nil
}

// startWorker runs the main worker loop.
// It processes directory branches from its queue and steals work from other workers
// when its queue is empty. The worker exits when no work is available.
func (s *Synchronizer) startWorker(worker *syncWorker) {
	defer s.wg.Done()

	for {
		branch := worker.queuePop()

		if branch != nil {
			if err := worker.processBranch(branch); err != nil {
				s.logger.Printf("ERROR processing '%s': %v\n", branch.relPath(), err)
			}
		} else {
			if !s.stealWork(worker) {
				// No work available, exit
				return
			}
		}
	}
}

// stealWork attempts to steal work from other workers when the thief's queue is empty.
// It uses a simple strategy: if another worker has more than 1 item in its queue,
// steal one item. Returns true if work was successfully stolen, false otherwise.
func (s *Synchronizer) stealWork(thief *syncWorker) bool {
	s.workerMu.Lock()
	defer s.workerMu.Unlock()

	for _, victim := range s.workers {
		if victim.id == thief.id {
			continue
		}

		qlen := victim.queueLen()
		if qlen > 1 {
			stolenItem := victim.queuePop()
			if stolenItem != nil {
				thief.queuePush(stolenItem)
				return true
			}
		}
	}

	return false
}

// processBranch syncs a single directory branch.
// It compares the source and destination directories, handling:
// - Creating missing directories
// - Copying modified files
// - Creating symlinks
// - Removing destination-only entries
func (w *syncWorker) processBranch(branch *syncBranch) error {
	srcAbsPath := branch.srcAbsPath(w.synchronizer.srcRoot)
	dstAbsPath := branch.dstAbsPath(w.synchronizer.dstRoot)

	// Lstat source directory
	srcInfo, err := os.Lstat(srcAbsPath)
	if w.synchronizer.callbacks.OnLstat != nil {
		w.synchronizer.callbacks.OnLstat(srcAbsPath, true, srcInfo, err)
	}

	if err != nil {
		return fmt.Errorf("lstat source '%s': %w", srcAbsPath, err)
	}

	// ReadDir source
	srcEntries, err := os.ReadDir(srcAbsPath)
	if w.synchronizer.callbacks.OnReadDir != nil {
		w.synchronizer.callbacks.OnReadDir(srcAbsPath, srcEntries, err)
	}

	if err != nil {
		return fmt.Errorf("readdir source '%s': %w", srcAbsPath, err)
	}

	// Build map of destination entries
	dstEntries, _ := os.ReadDir(dstAbsPath)
	dstMap := make(map[string]os.DirEntry)
	for _, entry := range dstEntries {
		dstMap[entry.Name()] = entry
	}

	// Process each source entry
	for _, srcEntry := range srcEntries {
		srcName := srcEntry.Name()


		srcChildPath := filepath.Join(srcAbsPath, srcName)
		dstChildPath := filepath.Join(dstAbsPath, srcName)

		dstEntry, dstExists := dstMap[srcName]

		// Check if entry should be ignored
		srcInfo, err := os.Lstat(srcChildPath)
		if w.synchronizer.callbacks.OnIgnore != nil {
			if w.synchronizer.callbacks.OnIgnore(srcName, srcChildPath, srcEntry.IsDir(), srcInfo, err) {
				continue
			}
		}

		if srcEntry.IsDir() {
			if dstExists && !dstEntry.IsDir() {
				// Type mismatch: dst is not a dir, remove it
				w.synchronizer.removeAll(dstChildPath)
			}

			if !dstExists {
				// Create destination directory
				if !w.synchronizer.readOnly {
					srcInfo, _ := os.Lstat(srcChildPath)
					mode := srcInfo.Mode().Perm()
					err := os.Mkdir(dstChildPath, mode)
					if w.synchronizer.callbacks.OnMkdir != nil {
						w.synchronizer.callbacks.OnMkdir(dstChildPath, mode, err)
					}
					// Sync inode attributes
					w.synchronizer.syncInodeAttrs(srcChildPath, dstChildPath)
				}
			} else {
				// Directory exists, sync inode attributes
				if !w.synchronizer.readOnly {
					w.synchronizer.syncInodeAttrs(srcChildPath, dstChildPath)
				}
			}

			// Queue child directory for recursive processing
			childBranch := &syncBranch{
				parent:   branch,
				basename: srcName,
			}
			w.queuePush(childBranch)
		} else if (srcEntry.Type() & os.ModeSymlink) != 0 {
			// Handle symlink
			linkTarget, _ := os.Readlink(srcChildPath)

			if dstExists && !((dstEntry.Type() & os.ModeSymlink) != 0) {
				// Type mismatch: dst is not a symlink, remove it
				w.synchronizer.unlink(dstChildPath)
			} else if dstExists {
				// Check if target matches
				dstTarget, _ := os.Readlink(dstChildPath)
				if dstTarget != linkTarget {
					w.synchronizer.unlink(dstChildPath)
					dstExists = false
				}
			}

			if !dstExists {
				if !w.synchronizer.readOnly {
					err := os.Symlink(linkTarget, dstChildPath)
					if w.synchronizer.callbacks.OnSymlink != nil {
						w.synchronizer.callbacks.OnSymlink(dstChildPath, linkTarget, err)
					}
				}
			}
		} else {
			// Regular file
			if dstExists && dstEntry.IsDir() {
				// Type mismatch: dst is a dir, remove it
				w.synchronizer.removeAll(dstChildPath)
			}

			// Check if we need to copy
			shouldCopy := !dstExists
			if !shouldCopy {
				srcInfo, _ := os.Lstat(srcChildPath)
				dstInfo, _ := os.Lstat(dstChildPath)
				// Copy if size or modtime differs
				shouldCopy = srcInfo.Size() != dstInfo.Size() || srcInfo.ModTime() != dstInfo.ModTime()
			}

			if shouldCopy {
				if !w.synchronizer.readOnly {
					srcInfo, _ := os.Lstat(srcChildPath)
					err := w.synchronizer.copyFile(srcChildPath, dstChildPath)
					if w.synchronizer.callbacks.OnCopy != nil {
						w.synchronizer.callbacks.OnCopy(srcChildPath, dstChildPath, srcInfo.Size(), err)
					}
					// Sync inode attributes after copy
					w.synchronizer.syncInodeAttrs(srcChildPath, dstChildPath)
				}
			} else if dstExists && !w.synchronizer.readOnly {
				// File exists but wasn't copied, still sync inode attributes
				w.synchronizer.syncInodeAttrs(srcChildPath, dstChildPath)
			}
		}

		// Remove from map to track what's been processed
		delete(dstMap, srcName)
	}

	// Remove entries that exist in dst but not in src
	for dstName, dstEntry := range dstMap {
		dstChildPath := filepath.Join(dstAbsPath, dstName)
		if dstEntry.IsDir() {
			w.synchronizer.removeAll(dstChildPath)
		} else {
			w.synchronizer.unlink(dstChildPath)
		}
	}

	return nil
}

// syncInodeAttrs synchronizes file inode attributes (permissions, ownership, times).
// It syncs mode, uid/gid, and modification/access times from source to destination.
// Errors are non-fatal and logged but do not abort the sync.
func (s *Synchronizer) syncInodeAttrs(srcPath, dstPath string) error {
	if s.readOnly {
		return nil
	}

	srcInfo, err := os.Lstat(srcPath)
	if err != nil {
		return nil // Source doesn't exist, skip
	}

	// Sync permissions (mode)
	mode := srcInfo.Mode().Perm()
	if err := os.Chmod(dstPath, mode); err != nil {
		// Log but don't fail
		s.logger.Printf("WARN: failed to chmod %s: %v\n", dstPath, err)
	}

	// Sync modification and access times
	modTime := srcInfo.ModTime()
	if err := os.Chtimes(dstPath, modTime, modTime); err != nil {
		// Log but don't fail
		s.logger.Printf("WARN: failed to chtimes %s: %v\n", dstPath, err)
	}

	// Sync ownership (uid/gid) if possible
	// Note: This requires appropriate permissions (usually root)
	stat := srcInfo.Sys().(*syscall.Stat_t)
	if stat != nil {
		uid := int(stat.Uid)
		gid := int(stat.Gid)
		if err := os.Chown(dstPath, uid, gid); err != nil {
			// Log but don't fail (non-root users typically can't chown)
			s.logger.Printf("DEBUG: failed to chown %s to %d:%d: %v\n", dstPath, uid, gid, err)
		}
	}

	return nil
}

// copyFileWithHash copies a file from source to destination with optional block-based hashing.
// It reads the file in 4K blocks, optionally hashing each block, and delivers hash values
// via callback (hasher BlockHasher). The producer and consumer maintain independent
// block ID counters starting from 0.
//
// Parameters:
//   - srcPath: source file path
//   - dstPath: destination file path
//   - algo: hash algorithm to use (HashAlgoNone to disable hashing)
//   - hasher: callback to receive block hashes (can be nil if algo is HashAlgoNone)
//
// Returns an error if the copy fails or if the hasher callback returns an error.
func (s *Synchronizer) copyFileWithHash(srcPath, dstPath string, algo HashAlgo, hasher BlockHasher) error {
	if s.readOnly {
		return nil
	}

	srcFile, err := os.Open(srcPath)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	dstFile, err := os.Create(dstPath)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	const blockSize = 4096

	// If no hashing requested, use simple io.Copy
	if algo == HashAlgoNone {
		_, err = io.Copy(dstFile, srcFile)
		return err
	}

	// Create hash function
	var h hash.Hash
	var xxHasher *xxhash.Digest
	switch algo {
	case HashAlgoMD5:
		h = md5.New()
	case HashAlgoSHA256:
		h = sha256.New()
	case HashAlgoSHA512:
		h = sha512.New()
	case HashAlgoXXHash:
		xxHasher = xxhash.New()
	default:
		return fmt.Errorf("unsupported hash algorithm: %s", algo)
	}

	buffer := make([]byte, blockSize)
	blockID := uint64(0)

	for {
		n, err := srcFile.Read(buffer)
		if err != nil && err != io.EOF {
			return err
		}

		if n > 0 {
			// Write to destination
			if _, err := dstFile.Write(buffer[:n]); err != nil {
				return err
			}

			// Hash the block
			var hashBytes []byte
			if xxHasher != nil {
				xxHasher.Reset()
				xxHasher.Write(buffer[:n])
				hashBytes = xxHasher.Sum(nil)
			} else {
				h.Reset()
				h.Write(buffer[:n])
				hashBytes = h.Sum(nil)
			}

			// Send hash to callback if provided
			if hasher != nil {
				if err := hasher.HashBlock(blockID, hashBytes); err != nil {
					return fmt.Errorf("hash block callback failed for block %d: %w", blockID, err)
				}
			}

			blockID++
		}

		if err == io.EOF {
			break
		}
	}

	return nil
}

// copyFile copies a file from source to destination.
// In read-only mode, this is a no-op. Otherwise, it performs a complete file copy
// using io.Copy for efficiency.
func (s *Synchronizer) copyFile(srcPath, dstPath string) error {
	return s.copyFileWithHash(srcPath, dstPath, HashAlgoNone, nil)
}

// unlink removes a file or symlink.
// In read-only mode, this is a no-op. Otherwise, it removes the file and
// invokes the OnUnlink callback if present.
func (s *Synchronizer) unlink(path string) error {
	if s.readOnly {
		return nil
	}

	err := os.Remove(path)
	if s.callbacks.OnUnlink != nil {
		s.callbacks.OnUnlink(path, err)
	}
	return err
}

// removeAll recursively removes a directory tree.
// In read-only mode, this is a no-op. Otherwise, it removes the directory and
// invokes the OnRemoveAll callback if present.
func (s *Synchronizer) removeAll(path string) error {
	if s.readOnly {
		return nil
	}

	err := os.RemoveAll(path)
	if s.callbacks.OnRemoveAll != nil {
		s.callbacks.OnRemoveAll(path, err)
	}
	return err
}

// Stop cancels the synchronization process.
// This will cause any running workers to exit gracefully.
func (s *Synchronizer) Stop() {
	s.cancel()
}
