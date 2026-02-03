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
	"sync/atomic"
	"syscall"
	"time"

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
	// OnSrcLstat is called for each source entry discovered via ReadDir (using entry.Info()).
	// Called with the path being examined, whether it's a directory, file info, and any error.
	OnSrcLstat func(path string, isDir bool, fileInfo os.FileInfo, err error)

	// OnDstLstat is called for each destination entry during comparison.
	// Called with the path being examined, whether it's a directory, file info, and any error.
	OnDstLstat func(path string, isDir bool, fileInfo os.FileInfo, err error)

	// OnSrcReadDir is called after reading a source directory.
	// Called with the directory path, its entries, and any error encountered.
	OnSrcReadDir func(path string, entries []os.DirEntry, err error)

	// OnDstReadDir is called after reading a destination directory.
	// Called with the directory path, its entries, and any error encountered.
	OnDstReadDir func(path string, entries []os.DirEntry, err error)

	// OnCopy is called before and after copying a file.
	// If err is nil, the copy was successful. Called with source path, destination path,
	// file size in bytes, and any error.
	OnCopy func(srcPath, dstPath string, size int64, err error)

	// OnDstMkdir is called before and after creating a destination directory.
	// Called with the directory path, file mode, and any error.
	OnDstMkdir func(path string, mode os.FileMode, err error)

	// OnDstUnlink is called before and after removing a destination file.
	// Called with the file path and any error encountered during removal.
	OnDstUnlink func(path string, err error)

	// OnDstRmdir is called before and after removing an empty destination directory.
	// Called with the directory path and any error.
	OnDstRmdir func(path string, err error)

	// OnDstRemoveAll is called when recursively removing a destination path.
	// Called with the path being removed and any error encountered.
	OnDstRemoveAll func(path string, err error)

	// OnDstSymlink is called when creating a destination symlink.
	// Called with the symlink path, target, and any error.
	OnDstSymlink func(linkPath, target string, err error)

	// OnDstChmod is called when changing destination permissions.
	// Called with the path, new file mode, and any error.
	OnDstChmod func(path string, mode os.FileMode, err error)

	// OnDstChmodDetail is called when changing destination permissions, with both the previous and new mode.
	OnDstChmodDetail func(path string, before, after os.FileMode, err error)

	// OnDstChown is called when changing destination ownership.
	// Called with the path, uid, gid, and any error.
	OnDstChown func(path string, uid, gid int, err error)

	// OnDstChownDetail is called when changing destination ownership with both the previous and new IDs.
	OnDstChownDetail func(path string, oldUID, oldGID, newUID, newGID int, err error)

	// OnDstChtimes is called when changing destination modification/access times.
	// Called with the path and any error encountered.
	OnDstChtimes func(path string, err error)

	// OnDstChtimesDetail is called when changing destination times with both the previous and new atime/mtime
	// and booleans indicating which times needed an update.
	OnDstChtimesDetail func(path string, beforeAtime, beforeMtime, afterAtime, afterMtime time.Time, changedAtime, changedMtime bool, err error)

	// OnDstUnlinkDetail is called when removing a destination file or symlink, with the size of the removed file.
	// Called with the file path, file size in bytes, and any error encountered.
	OnDstUnlinkDetail func(path string, size int64, err error)

	// OnDstRemoveAllDetail is called when recursively removing a destination path, with the total size removed.
	// Called with the path being removed, total size in bytes, and any error encountered.
	OnDstRemoveAllDetail func(path string, size int64, err error)

	// OnIgnore is called to determine if a source file or directory should be ignored during sync.
	// Called with the entry name (basename), full absolute path, whether it's a directory,
	// the stat information (from entry.Info or Lstat), and any stat error.
	// Return true to skip this entry, false to process it.
	// If OnIgnore is nil, no entries are ignored by default.
	OnIgnore func(name string, path string, isDir bool, fileInfo os.FileInfo, err error) bool
}

// Options configures optional synchronizer behaviors.
type Options struct {
	// IgnoreAtime skips considering atime differences when deciding whether to
	// call Chtimes and preserves existing atime when possible.
	IgnoreAtime bool
	// LogSlowOps logs when an operation takes longer than 1 second.
	LogSlowOps bool
	// SlowOpInterval controls how often slow operations are logged.
	// Defaults to 1s when LogSlowOps is enabled.
	SlowOpInterval time.Duration
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
	options   Options

	monitorCtx context.Context
	cancel     context.CancelFunc

	// Worker pool management
	numWorkers int
	workers    []*syncWorker
	workerMu   sync.Mutex
	wg         sync.WaitGroup
	shutdown   int32

	rootReadOnce sync.Once
	rootReadDone chan struct{}

	// In-progress operation tracking for concurrency monitoring
	inProgressLstat     atomic.Uint64
	inProgressReaddir   atomic.Uint64
	inProgressMkdir     atomic.Uint64
	inProgressUnlink    atomic.Uint64
	inProgressRemoveAll atomic.Uint64
	inProgressSymlink   atomic.Uint64
	inProgressChmod     atomic.Uint64
	inProgressChown     atomic.Uint64
	inProgressChtimes   atomic.Uint64
	inProgressCopy      atomic.Uint64
}

// syncWorker represents a single worker processing directories.
// Each worker maintains a queue of directory branches to process and cooperates
// with other workers through the work stealing mechanism.
type syncWorker struct {
	id           int
	synchronizer *Synchronizer
	queue        []*syncBranch
	mu           sync.Mutex
	stateMu      sync.Mutex
	state        workerStateInternal
}

type workerStateInternal struct {
	state    string
	op       string
	path     string
	branch   string
	started  time.Time
	lastSlow time.Time
	SideSrc  bool
	SideDst  bool
}

// WorkerState exposes the current state of a worker for monitoring.
type WorkerState struct {
	ID        int
	State     string
	Operation string
	Path      string
	Since     time.Time
	Duration  time.Duration
	QueueLen  int
	SideSrc   bool
	SideDst   bool
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

func (sw *syncWorker) setIdle() {
	sw.stateMu.Lock()
	defer sw.stateMu.Unlock()
	sw.state = workerStateInternal{state: "idle"}
}

func (sw *syncWorker) setSteal() {
	sw.stateMu.Lock()
	defer sw.stateMu.Unlock()
	sw.state = workerStateInternal{state: "steal", started: time.Now()}
}

func (sw *syncWorker) setBranch(path string) {
	sw.stateMu.Lock()
	defer sw.stateMu.Unlock()
	sw.state.state = "processing"
	sw.state.branch = path
	if sw.state.op == "" {
		sw.state.path = path
	}
}

func (sw *syncWorker) beginOp(op, path string, sideSrc, sideDst bool) {
	sw.stateMu.Lock()
	defer sw.stateMu.Unlock()
	sw.state.state = op
	sw.state.op = op
	sw.state.path = path
	sw.state.started = time.Now()
	sw.state.lastSlow = time.Time{}
	sw.state.SideSrc = sideSrc
	sw.state.SideDst = sideDst
}

func (sw *syncWorker) endOp() {
	sw.stateMu.Lock()
	defer sw.stateMu.Unlock()
	sw.state.op = ""
	sw.state.started = time.Time{}
	sw.state.lastSlow = time.Time{}
	if sw.state.branch != "" {
		sw.state.state = "processing"
		sw.state.path = sw.state.branch
	} else {
		sw.state.state = "idle"
		sw.state.path = ""
	}
}

func (sw *syncWorker) snapshotState() WorkerState {
	sw.stateMu.Lock()
	state := sw.state
	sw.stateMu.Unlock()

	op := state.op
	if op == "" {
		if state.state == "processing" && state.branch != "" {
			op = "branch"
		} else {
			op = state.state
		}
	}

	ws := WorkerState{
		ID:        sw.id,
		State:     state.state,
		Operation: op,
		Path:      state.path,
		Since:     state.started,
		QueueLen:  sw.queueLen(),
		SideSrc:   state.SideSrc,
		SideDst:   state.SideDst,
	}
	if !state.started.IsZero() {
		ws.Duration = time.Since(state.started)
	}
	return ws
}

func (sw *syncWorker) markSlowLogged(now time.Time) {
	sw.stateMu.Lock()
	sw.state.lastSlow = now
	sw.stateMu.Unlock()
}

func (sw *syncWorker) shouldLogSlow(now time.Time, threshold, interval time.Duration) (bool, workerStateInternal) {
	sw.stateMu.Lock()
	defer sw.stateMu.Unlock()
	state := sw.state
	if state.op == "" || state.started.IsZero() {
		return false, state
	}
	if now.Sub(state.started) < threshold {
		return false, state
	}
	if !state.lastSlow.IsZero() && now.Sub(state.lastSlow) < interval {
		return false, state
	}
	return true, state
}

// NewSynchronizer creates a new Synchronizer for syncing srcRoot to dstRoot.
// numWorkers specifies the number of parallel workers to use (minimum 1).
// If numWorkers <= 0, it defaults to 1.
// readOnly, when true, prevents any modifications to the destination.
// callbacks can be used to monitor sync operations; nil callbacks are ignored.
// logger is used for logging errors and progress; if nil, a default logger is used.
func NewSynchronizer(srcRoot, dstRoot string, numWorkers int, readOnly bool, callbacks Callbacks) *Synchronizer {
	return NewSynchronizerWithLoggerAndOptions(srcRoot, dstRoot, numWorkers, readOnly, callbacks, nil, Options{})
}

// NewSynchronizerWithLogger creates a new Synchronizer with a custom logger.
// If logger is nil, a default logger wrapping the standard log package is used.
func NewSynchronizerWithLogger(srcRoot, dstRoot string, numWorkers int, readOnly bool, callbacks Callbacks, logger Logger) *Synchronizer {
	return NewSynchronizerWithLoggerAndOptions(srcRoot, dstRoot, numWorkers, readOnly, callbacks, logger, Options{})
}

// NewSynchronizerWithLoggerAndOptions creates a new Synchronizer with a custom logger and options.
// If logger is nil, a default logger wrapping the standard log package is used.
func NewSynchronizerWithLoggerAndOptions(srcRoot, dstRoot string, numWorkers int, readOnly bool, callbacks Callbacks, logger Logger, opts Options) *Synchronizer {
	if numWorkers <= 0 {
		numWorkers = 1
	}

	if logger == nil {
		logger = &defaultLogger{}
	}

	if opts.LogSlowOps && opts.SlowOpInterval == 0 {
		opts.SlowOpInterval = time.Second
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Synchronizer{
		srcRoot:      filepath.Clean(srcRoot),
		dstRoot:      filepath.Clean(dstRoot),
		callbacks:    callbacks,
		readOnly:     readOnly,
		logger:       logger,
		options:      opts,
		monitorCtx:   ctx,
		cancel:       cancel,
		numWorkers:   numWorkers,
		rootReadDone: make(chan struct{}),
	}
}

func (s *Synchronizer) signalRootRead() {
	s.rootReadOnce.Do(func() {
		close(s.rootReadDone)
	})
}

func (s *Synchronizer) startOp(worker *syncWorker, opName, path string) {
	if worker != nil {
		sideSrc, sideDst := s.opSide(path)
		worker.beginOp(opName, path, sideSrc, sideDst)
	}
	s.trackOpStart(opName)
}

func (s *Synchronizer) startOpSide(worker *syncWorker, opName, path string, sideSrc, sideDst bool) {
	if worker != nil {
		worker.beginOp(opName, path, sideSrc, sideDst)
	}
	s.trackOpStart(opName)
}

func (s *Synchronizer) startSrcOp(worker *syncWorker, opName, path string) {
	s.startOpSide(worker, opName, path, true, false)
}

func (s *Synchronizer) startDstOp(worker *syncWorker, opName, path string) {
	s.startOpSide(worker, opName, path, false, true)
}

func (s *Synchronizer) startCopyOp(worker *syncWorker, srcPath, dstPath string) {
	if worker != nil {
		s.startOpSide(worker, "copy", srcPath, true, true)
		return
	}
	s.trackOpStart("copy")
}

func (s *Synchronizer) endOp(worker *syncWorker, opName string) {
	s.trackOpEnd(opName)
	if worker != nil {
		worker.endOp()
	}
}

func (s *Synchronizer) isUnderRoot(path, root string) bool {
	if path == root {
		return true
	}
	rootWithSep := root + string(os.PathSeparator)
	return strings.HasPrefix(path, rootWithSep)
}

func (s *Synchronizer) opSide(path string) (bool, bool) {
	return s.isUnderRoot(path, s.srcRoot), s.isUnderRoot(path, s.dstRoot)
}

// trackOpStart increments the in-progress counter for an operation
func (s *Synchronizer) trackOpStart(opName string) {
	switch opName {
	case "lstat":
		s.inProgressLstat.Add(1)
	case "readdir":
		s.inProgressReaddir.Add(1)
	case "mkdir":
		s.inProgressMkdir.Add(1)
	case "unlink":
		s.inProgressUnlink.Add(1)
	case "removeall":
		s.inProgressRemoveAll.Add(1)
	case "symlink":
		s.inProgressSymlink.Add(1)
	case "chmod":
		s.inProgressChmod.Add(1)
	case "chown":
		s.inProgressChown.Add(1)
	case "chtimes":
		s.inProgressChtimes.Add(1)
	case "copy":
		s.inProgressCopy.Add(1)
	}
}

// trackOpEnd decrements the in-progress counter for an operation
func (s *Synchronizer) trackOpEnd(opName string) {
	switch opName {
	case "lstat":
		s.inProgressLstat.Add(^uint64(0)) // atomic decrement
	case "readdir":
		s.inProgressReaddir.Add(^uint64(0))
	case "mkdir":
		s.inProgressMkdir.Add(^uint64(0))
	case "unlink":
		s.inProgressUnlink.Add(^uint64(0))
	case "removeall":
		s.inProgressRemoveAll.Add(^uint64(0))
	case "symlink":
		s.inProgressSymlink.Add(^uint64(0))
	case "chmod":
		s.inProgressChmod.Add(^uint64(0))
	case "chown":
		s.inProgressChown.Add(^uint64(0))
	case "chtimes":
		s.inProgressChtimes.Add(^uint64(0))
	case "copy":
		s.inProgressCopy.Add(^uint64(0))
	}
}

// GetInProgressOperations returns the current counts of in-progress operations
func (s *Synchronizer) GetInProgressOperations() map[string]uint64 {
	return map[string]uint64{
		"lstat":     s.inProgressLstat.Load(),
		"readdir":   s.inProgressReaddir.Load(),
		"mkdir":     s.inProgressMkdir.Load(),
		"unlink":    s.inProgressUnlink.Load(),
		"removeall": s.inProgressRemoveAll.Load(),
		"symlink":   s.inProgressSymlink.Load(),
		"chmod":     s.inProgressChmod.Load(),
		"chown":     s.inProgressChown.Load(),
		"chtimes":   s.inProgressChtimes.Load(),
		"copy":      s.inProgressCopy.Load(),
	}
}

// GetWorkerStates returns the current state of all workers.
func (s *Synchronizer) GetWorkerStates() []WorkerState {
	s.workerMu.Lock()
	workers := append([]*syncWorker(nil), s.workers...)
	s.workerMu.Unlock()

	states := make([]WorkerState, 0, len(workers))
	for _, worker := range workers {
		states = append(states, worker.snapshotState())
	}
	return states
}

func (s *Synchronizer) logSlowOps() {
	interval := s.options.SlowOpInterval
	if interval <= 0 {
		interval = time.Second
	}
	threshold := time.Second

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-s.monitorCtx.Done():
			return
		case now := <-ticker.C:
			s.workerMu.Lock()
			workers := append([]*syncWorker(nil), s.workers...)
			s.workerMu.Unlock()
			for _, worker := range workers {
				shouldLog, state := worker.shouldLogSlow(now, threshold, interval)
				if !shouldLog {
					continue
				}
				duration := now.Sub(state.started)
				s.logger.Printf("WARN slow op: %s '%s' running %s (waiting for syscall to return)\n", state.op, state.path, duration.Truncate(time.Millisecond))
				worker.markSlowLogged(now)
			}
		}
	}
}

// Run starts the synchronization process.
// It initializes a worker pool and processes the directory tree in parallel.
// The function blocks until all workers complete.
// Returns an error if the synchronization fails.
func (s *Synchronizer) Run() error {
	defer s.cancel()
	// Initialize workers
	s.workerMu.Lock()
	for i := 0; i < s.numWorkers; i++ {
		worker := &syncWorker{
			id:           i,
			synchronizer: s,
		}
		s.workers = append(s.workers, worker)
	}
	s.workerMu.Unlock()

	if s.options.LogSlowOps {
		go s.logSlowOps()
	}

	// Start with root directory
	root := &syncBranch{}
	s.workers[0].queuePush(root)

	// Start worker 0 immediately
	s.wg.Add(1)
	go s.startWorker(s.workers[0])

	// Wait until both source and destination roots have been read before starting others
	<-s.rootReadDone

	// Start remaining workers staggered
	for i := 1; i < s.numWorkers; i++ {
		s.wg.Add(1)
		go s.startWorker(s.workers[i])
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for all workers to finish
	s.wg.Wait()

	return nil
}

// startWorker runs the main worker loop.
// It processes directory branches from its queue and steals work from other workers
// when its queue is empty. The worker exits when no work is available.
func (s *Synchronizer) startWorker(worker *syncWorker) {
	defer s.wg.Done()

	worker.setIdle()

	for {
		branch := worker.queuePop()

		if branch != nil {
			worker.setBranch(branch.relPath())
			if err := worker.processBranch(branch); err != nil {
				s.logger.Printf("ERROR processing '%s': %v\n", branch.relPath(), err)
			}
		} else {
			worker.setSteal()
			if !s.stealWork(worker) {
				worker.setIdle()
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

	// ReadDir source (collects stat info for entries internally)
	w.synchronizer.startSrcOp(w, "readdir", srcAbsPath)
	srcEntries, err := os.ReadDir(srcAbsPath)
	w.synchronizer.endOp(w, "readdir")
	if w.synchronizer.callbacks.OnSrcReadDir != nil {
		w.synchronizer.callbacks.OnSrcReadDir(srcAbsPath, srcEntries, err)
	}

	if err != nil {
		if branch.isRoot() {
			_, _ = os.ReadDir(dstAbsPath)
			w.synchronizer.signalRootRead()
		}
		return fmt.Errorf("readdir source '%s': %w", srcAbsPath, err)
	}

	// Emit OnLstat for each source entry using the info populated by ReadDir
	entryInfo := make(map[string]os.FileInfo, len(srcEntries))
	entryErr := make(map[string]error)
	if w.synchronizer.callbacks.OnSrcLstat != nil {
		for _, entry := range srcEntries {
			entryPath := filepath.Join(srcAbsPath, entry.Name())
			w.synchronizer.startSrcOp(w, "lstat", entryPath)
			info, infoErr := entry.Info()
			w.synchronizer.endOp(w, "lstat")
			w.synchronizer.callbacks.OnSrcLstat(filepath.Join(srcAbsPath, entry.Name()), entry.IsDir(), info, infoErr)
			if infoErr == nil {
				entryInfo[entry.Name()] = info
			} else {
				entryErr[entry.Name()] = infoErr
			}
		}
	}
	// Always track lstat for source entries
	for _, entry := range srcEntries {
		entryPath := filepath.Join(srcAbsPath, entry.Name())
		w.synchronizer.startSrcOp(w, "lstat", entryPath)
		info, infoErr := entry.Info()
		w.synchronizer.endOp(w, "lstat")
		if infoErr == nil {
			entryInfo[entry.Name()] = info
		} else {
			entryErr[entry.Name()] = infoErr
		}
	}

	getSrcInfo := func(name string) (os.FileInfo, error) {
		if info, ok := entryInfo[name]; ok {
			return info, nil
		}
		if err, ok := entryErr[name]; ok {
			return nil, err
		}
		entryPath := filepath.Join(srcAbsPath, name)
		w.synchronizer.startSrcOp(w, "lstat", entryPath)
		info, err := os.Lstat(entryPath)
		w.synchronizer.endOp(w, "lstat")
		if w.synchronizer.callbacks.OnSrcLstat != nil {
			w.synchronizer.callbacks.OnSrcLstat(entryPath, info != nil && info.IsDir(), info, err)
		}
		if err == nil {
			entryInfo[name] = info
		} else {
			entryErr[name] = err
		}
		return info, err
	}

	// Build map of destination entries
	w.synchronizer.startDstOp(w, "readdir", dstAbsPath)
	dstEntries, dstReadErr := os.ReadDir(dstAbsPath)
	w.synchronizer.endOp(w, "readdir")
	if w.synchronizer.callbacks.OnDstReadDir != nil {
		w.synchronizer.callbacks.OnDstReadDir(dstAbsPath, dstEntries, dstReadErr)
	}
	if branch.isRoot() {
		w.synchronizer.signalRootRead()
	}
	dstMap := make(map[string]os.DirEntry)

	// Emit OnDstLstat for each destination entry using the info populated by ReadDir
	if w.synchronizer.callbacks.OnDstLstat != nil {
		for _, entry := range dstEntries {
			entryPath := filepath.Join(dstAbsPath, entry.Name())
			w.synchronizer.startDstOp(w, "lstat", entryPath)
			info, infoErr := entry.Info()
			w.synchronizer.endOp(w, "lstat")
			w.synchronizer.callbacks.OnDstLstat(entryPath, entry.IsDir(), info, infoErr)
		}
	}

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
		srcInfo, err := getSrcInfo(srcName)
		if w.synchronizer.callbacks.OnIgnore != nil {
			if w.synchronizer.callbacks.OnIgnore(srcName, srcChildPath, srcEntry.IsDir(), srcInfo, err) {
				continue
			}
		}

		if srcEntry.IsDir() {
			if dstExists && !dstEntry.IsDir() {
				// Type mismatch: dst is not a dir, remove it
				w.synchronizer.removeAll(w, dstChildPath)
			}

			if !dstExists {
				// Create destination directory
				if !w.synchronizer.readOnly {
					srcInfo, _ := getSrcInfo(srcName)
					mode := srcInfo.Mode().Perm()
					w.synchronizer.startDstOp(w, "mkdir", dstChildPath)
					err := os.Mkdir(dstChildPath, mode)
					w.synchronizer.endOp(w, "mkdir")
					if w.synchronizer.callbacks.OnDstMkdir != nil {
						w.synchronizer.callbacks.OnDstMkdir(dstChildPath, mode, err)
					}
					// Sync inode attributes
					w.synchronizer.syncInodeAttrs(w, srcChildPath, dstChildPath)
				}
			} else {
				// Directory exists, sync inode attributes
				if !w.synchronizer.readOnly {
					w.synchronizer.syncInodeAttrs(w, srcChildPath, dstChildPath)
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
				var fileSize int64
				if info, err := dstEntry.Info(); err == nil {
					fileSize = info.Size()
				}
				w.synchronizer.unlinkWithSize(w, dstChildPath, fileSize)
			} else if dstExists {
				// Check if target matches
				dstTarget, _ := os.Readlink(dstChildPath)
				if dstTarget != linkTarget {
					var fileSize int64
					if info, err := dstEntry.Info(); err == nil {
						fileSize = info.Size()
					}
					w.synchronizer.unlinkWithSize(w, dstChildPath, fileSize)
					dstExists = false
				}
			}

			if !dstExists {
				if !w.synchronizer.readOnly {
					w.synchronizer.startDstOp(w, "symlink", dstChildPath)
					err := os.Symlink(linkTarget, dstChildPath)
					w.synchronizer.endOp(w, "symlink")
					if w.synchronizer.callbacks.OnDstSymlink != nil {
						w.synchronizer.callbacks.OnDstSymlink(dstChildPath, linkTarget, err)
					}
				}
			}
		} else {
			// Regular file
			if dstExists && dstEntry.IsDir() {
				// Type mismatch: dst is a dir, remove it
				var dirSize int64
				if info, err := dstEntry.Info(); err == nil {
					dirSize = info.Size()
				}
				w.synchronizer.removeAllWithSize(w, dstChildPath, dirSize)
			}

			// Check if we need to copy
			shouldCopy := !dstExists
			if !shouldCopy {
				srcInfo, _ := getSrcInfo(srcName)
				w.synchronizer.startDstOp(w, "lstat", dstChildPath)
				dstInfo, _ := os.Lstat(dstChildPath)
				w.synchronizer.endOp(w, "lstat")
				if w.synchronizer.callbacks.OnDstLstat != nil && dstInfo != nil {
					w.synchronizer.callbacks.OnDstLstat(dstChildPath, dstInfo.IsDir(), dstInfo, nil)
				}
				// Copy if size or modtime differs
				shouldCopy = srcInfo.Size() != dstInfo.Size() || srcInfo.ModTime() != dstInfo.ModTime()
			}

			if shouldCopy {
				if !w.synchronizer.readOnly {
					srcInfo, _ := getSrcInfo(srcName)
					w.synchronizer.startCopyOp(w, srcChildPath, dstChildPath)
					err := w.synchronizer.copyFile(srcChildPath, dstChildPath)
					w.synchronizer.endOp(w, "copy")
					if w.synchronizer.callbacks.OnCopy != nil {
						w.synchronizer.callbacks.OnCopy(srcChildPath, dstChildPath, srcInfo.Size(), err)
					}
					// Sync inode attributes after copy
					w.synchronizer.syncInodeAttrs(w, srcChildPath, dstChildPath)
				}
			} else if dstExists && !w.synchronizer.readOnly {
				// File exists but wasn't copied, still sync inode attributes
				w.synchronizer.syncInodeAttrs(w, srcChildPath, dstChildPath)
			}
		}

		// Remove from map to track what's been processed
		delete(dstMap, srcName)
	}

	// Remove entries that exist in dst but not in src
	for dstName, dstEntry := range dstMap {
		dstChildPath := filepath.Join(dstAbsPath, dstName)
		if dstEntry.IsDir() {
			// Calculate total size of directory tree before removal
			var totalSize int64
			if info, err := dstEntry.Info(); err == nil {
				totalSize = info.Size()
			}
			w.synchronizer.removeAllWithSize(w, dstChildPath, totalSize)
		} else {
			// Get file size before removal
			var fileSize int64
			if info, err := dstEntry.Info(); err == nil {
				fileSize = info.Size()
			}
			w.synchronizer.unlinkWithSize(w, dstChildPath, fileSize)
		}
	}

	return nil
}

// syncInodeAttrs synchronizes file inode attributes (permissions, ownership, times).
// It syncs mode, uid/gid, and modification/access times from source to destination.
// Errors are non-fatal and logged but do not abort the sync.
func (s *Synchronizer) syncInodeAttrs(worker *syncWorker, srcPath, dstPath string) error {
	if s.readOnly {
		return nil
	}

	srcInfo, err := os.Lstat(srcPath)
	if err != nil {
		return nil // Source doesn't exist, skip
	}

	var dstInfo os.FileInfo
	dstInfo, _ = os.Lstat(dstPath)

	var beforeMode os.FileMode
	var beforeAtime, beforeMtime time.Time
	var beforeUID, beforeGID int
	if dstInfo != nil {
		beforeMode = dstInfo.Mode().Perm()
		if stat, ok := dstInfo.Sys().(*syscall.Stat_t); ok {
			beforeUID = int(stat.Uid)
			beforeGID = int(stat.Gid)
			beforeAtime = time.Unix(stat.Atim.Sec, stat.Atim.Nsec)
			beforeMtime = time.Unix(stat.Mtim.Sec, stat.Mtim.Nsec)
		} else {
			beforeMtime = dstInfo.ModTime()
		}
	}

	// Sync permissions (mode)
	mode := srcInfo.Mode().Perm()
	if beforeMode != mode {
		s.startDstOp(worker, "chmod", dstPath)
		if err := os.Chmod(dstPath, mode); err != nil {
			s.endOp(worker, "chmod")
			// Log but don't fail
			s.logger.Printf("WARN: failed to chmod %s: %v\n", dstPath, err)
			if s.callbacks.OnDstChmod != nil {
				s.callbacks.OnDstChmod(dstPath, mode, err)
			}
			if s.callbacks.OnDstChmodDetail != nil {
				s.callbacks.OnDstChmodDetail(dstPath, beforeMode, mode, err)
			}
		} else {
			s.endOp(worker, "chmod")
			if s.callbacks.OnDstChmod != nil {
				s.callbacks.OnDstChmod(dstPath, mode, nil)
			}
			if s.callbacks.OnDstChmodDetail != nil {
				s.callbacks.OnDstChmodDetail(dstPath, beforeMode, mode, nil)
			}
		}
	}

	// Sync modification and access times (only for regular files)
	if srcInfo.Mode().IsRegular() {
		modTime := srcInfo.ModTime()
		changedAtime := beforeAtime.IsZero() || !beforeAtime.Equal(modTime)
		if s.options.IgnoreAtime {
			changedAtime = false
		}
		changedMtime := beforeMtime.IsZero() || !beforeMtime.Equal(modTime)
		if changedAtime || changedMtime {
			atimeToSet := modTime
			if s.options.IgnoreAtime && !beforeAtime.IsZero() {
				atimeToSet = beforeAtime
			}
			s.startDstOp(worker, "chtimes", dstPath)
			if err := os.Chtimes(dstPath, atimeToSet, modTime); err != nil {
				s.endOp(worker, "chtimes")
				// Log but don't fail
				s.logger.Printf("WARN: failed to chtimes %s: %v\n", dstPath, err)
				if s.callbacks.OnDstChtimes != nil {
					s.callbacks.OnDstChtimes(dstPath, err)
				}
				if s.callbacks.OnDstChtimesDetail != nil {
					s.callbacks.OnDstChtimesDetail(dstPath, beforeAtime, beforeMtime, atimeToSet, modTime, changedAtime, changedMtime, err)
				}
			} else {
				s.endOp(worker, "chtimes")
				if s.callbacks.OnDstChtimes != nil {
					s.callbacks.OnDstChtimes(dstPath, nil)
				}
				if s.callbacks.OnDstChtimesDetail != nil {
					s.callbacks.OnDstChtimesDetail(dstPath, beforeAtime, beforeMtime, atimeToSet, modTime, changedAtime, changedMtime, nil)
				}
			}
		}
	}

	// Sync ownership (uid/gid) if possible
	// Note: This requires appropriate permissions (usually root)
	stat := srcInfo.Sys().(*syscall.Stat_t)
	if stat != nil {
		uid := int(stat.Uid)
		gid := int(stat.Gid)
		if beforeUID != uid || beforeGID != gid {
			s.startDstOp(worker, "chown", dstPath)
			if err := os.Chown(dstPath, uid, gid); err != nil {
				s.endOp(worker, "chown")
				// Log but don't fail (non-root users typically can't chown)
				s.logger.Printf("DEBUG: failed to chown %s to %d:%d: %v\n", dstPath, uid, gid, err)
				if s.callbacks.OnDstChown != nil {
					s.callbacks.OnDstChown(dstPath, uid, gid, err)
				}
				if s.callbacks.OnDstChownDetail != nil {
					s.callbacks.OnDstChownDetail(dstPath, beforeUID, beforeGID, uid, gid, err)
				}
			} else {
				s.endOp(worker, "chown")
				if s.callbacks.OnDstChown != nil {
					s.callbacks.OnDstChown(dstPath, uid, gid, nil)
				}
				if s.callbacks.OnDstChownDetail != nil {
					s.callbacks.OnDstChownDetail(dstPath, beforeUID, beforeGID, uid, gid, nil)
				}
			}
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
func (s *Synchronizer) unlink(worker *syncWorker, path string) error {
	if s.readOnly {
		return nil
	}

	s.startDstOp(worker, "unlink", path)
	err := os.Remove(path)
	s.endOp(worker, "unlink")
	if s.callbacks.OnDstUnlink != nil {
		s.callbacks.OnDstUnlink(path, err)
	}
	return err
}

// unlinkWithSize removes a file or symlink with size information for detailed tracking.
// Calls both OnDstUnlink and OnDstUnlinkDetail callbacks if present.
func (s *Synchronizer) unlinkWithSize(worker *syncWorker, path string, size int64) error {
	if s.readOnly {
		return nil
	}

	s.startDstOp(worker, "unlink", path)
	err := os.Remove(path)
	s.endOp(worker, "unlink")
	if s.callbacks.OnDstUnlink != nil {
		s.callbacks.OnDstUnlink(path, err)
	}
	if s.callbacks.OnDstUnlinkDetail != nil {
		s.callbacks.OnDstUnlinkDetail(path, size, err)
	}
	return err
}

// removeAll recursively removes a directory tree.
// In read-only mode, this is a no-op. Otherwise, it removes the directory and
// invokes the OnRemoveAll callback if present.
func (s *Synchronizer) removeAll(worker *syncWorker, path string) error {
	if s.readOnly {
		return nil
	}

	s.startDstOp(worker, "removeall", path)
	err := os.RemoveAll(path)
	s.endOp(worker, "removeall")
	if s.callbacks.OnDstRemoveAll != nil {
		s.callbacks.OnDstRemoveAll(path, err)
	}
	return err
}

// removeAllWithSize recursively removes a directory tree with size information for detailed tracking.
// Calls both OnDstRemoveAll and OnDstRemoveAllDetail callbacks if present.
func (s *Synchronizer) removeAllWithSize(worker *syncWorker, path string, size int64) error {
	if s.readOnly {
		return nil
	}

	s.startDstOp(worker, "removeall", path)
	err := os.RemoveAll(path)
	s.endOp(worker, "removeall")
	if s.callbacks.OnDstRemoveAll != nil {
		s.callbacks.OnDstRemoveAll(path, err)
	}
	if s.callbacks.OnDstRemoveAllDetail != nil {
		s.callbacks.OnDstRemoveAllDetail(path, size, err)
	}
	return err
}

// Stop cancels the synchronization process.
// This will cause any running workers to exit gracefully.
func (s *Synchronizer) Stop() {
	s.cancel()
}
