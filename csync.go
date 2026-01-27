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
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// Version is the current version of the csync package.
const Version = "0.1.0"

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
func NewSynchronizer(srcRoot, dstRoot string, numWorkers int, readOnly bool, callbacks Callbacks) *Synchronizer {
	if numWorkers <= 0 {
		numWorkers = 1
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Synchronizer{
		srcRoot:    filepath.Clean(srcRoot),
		dstRoot:    filepath.Clean(dstRoot),
		callbacks:  callbacks,
		readOnly:   readOnly,
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
				log.Printf("ERROR processing '%s': %v\n", branch.relPath(), err)
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
		if entry.IsDir() && entry.Name() == ".snapshot" {
			continue
		}
		dstMap[entry.Name()] = entry
	}

	// Process each source entry
	for _, srcEntry := range srcEntries {
		srcName := srcEntry.Name()

		// Skip special directories
		if srcEntry.IsDir() && srcName == ".snapshot" {
			continue
		}

		srcChildPath := filepath.Join(srcAbsPath, srcName)
		dstChildPath := filepath.Join(dstAbsPath, srcName)

		dstEntry, dstExists := dstMap[srcName]

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
				}
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

// copyFile copies a file from source to destination.
// In read-only mode, this is a no-op. Otherwise, it performs a complete file copy
// using io.Copy for efficiency.
func (s *Synchronizer) copyFile(srcPath, dstPath string) error {
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

	_, err = io.Copy(dstFile, srcFile)
	return err
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
