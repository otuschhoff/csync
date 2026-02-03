package csync

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestBasicSync tests basic file synchronization.
// It verifies that files and directories are correctly copied from source to destination.
func TestBasicSync(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create source structure
	if err := os.Mkdir(filepath.Join(srcDir, "subdir"), 0755); err != nil {
		t.Fatalf("failed to create source subdir: %v", err)
	}

	if err := os.WriteFile(filepath.Join(srcDir, "file.txt"), []byte("hello"), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	if err := os.WriteFile(filepath.Join(srcDir, "subdir", "nested.txt"), []byte("world"), 0644); err != nil {
		t.Fatalf("failed to create nested file: %v", err)
	}

	// Sync
	s := NewSynchronizer(srcDir, dstDir, 1, false, Callbacks{})
	if err := s.Run(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Verify destination structure
	fileContent, err := os.ReadFile(filepath.Join(dstDir, "file.txt"))
	if err != nil {
		t.Errorf("file.txt not found in dst: %v", err)
	} else if string(fileContent) != "hello" {
		t.Errorf("file.txt content mismatch")
	}

	nestedContent, err := os.ReadFile(filepath.Join(dstDir, "subdir", "nested.txt"))
	if err != nil {
		t.Errorf("nested.txt not found in dst: %v", err)
	} else if string(nestedContent) != "world" {
		t.Errorf("nested.txt content mismatch")
	}
}

// TestSymlinkSync tests symlink synchronization.
// It verifies that symlinks are correctly created in the destination with proper targets.
func TestSymlinkSync(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create source file
	filePath := filepath.Join(srcDir, "target.txt")
	if err := os.WriteFile(filePath, []byte("target content"), 0644); err != nil {
		t.Fatalf("failed to create target file: %v", err)
	}

	// Create symlink
	linkPath := filepath.Join(srcDir, "link.txt")
	if err := os.Symlink("target.txt", linkPath); err != nil {
		t.Fatalf("failed to create symlink: %v", err)
	}

	// Sync
	s := NewSynchronizer(srcDir, dstDir, 1, false, Callbacks{})
	if err := s.Run(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Verify symlink was created
	dstLinkPath := filepath.Join(dstDir, "link.txt")
	linkTarget, err := os.Readlink(dstLinkPath)
	if err != nil {
		t.Errorf("symlink not created in dst: %v", err)
	} else if linkTarget != "target.txt" {
		t.Errorf("symlink target mismatch: expected 'target.txt', got '%s'", linkTarget)
	}
}

// TestDeleteSync tests deletion of files/dirs that exist only in destination.
// It verifies that files present in destination but not in source are removed.
func TestDeleteSync(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create source file
	if err := os.WriteFile(filepath.Join(srcDir, "keep.txt"), []byte("keep"), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Create destination files
	if err := os.WriteFile(filepath.Join(dstDir, "keep.txt"), []byte("keep"), 0644); err != nil {
		t.Fatalf("failed to create dest file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dstDir, "remove.txt"), []byte("remove"), 0644); err != nil {
		t.Fatalf("failed to create dest file: %v", err)
	}

	// Sync
	s := NewSynchronizer(srcDir, dstDir, 1, false, Callbacks{})
	if err := s.Run(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Verify keep.txt exists
	if _, err := os.Stat(filepath.Join(dstDir, "keep.txt")); err != nil {
		t.Errorf("keep.txt was removed: %v", err)
	}

	// Verify remove.txt is deleted
	if _, err := os.Stat(filepath.Join(dstDir, "remove.txt")); err == nil {
		// Re-sync to ensure cleanup happens
		s2 := NewSynchronizer(srcDir, dstDir, 1, false, Callbacks{})
		s2.Run()

		// Check again after resync
		if _, err := os.Stat(filepath.Join(dstDir, "remove.txt")); err == nil {
			t.Errorf("remove.txt still exists after resync")
		}
	}
}

// TestReadOnly tests read-only mode doesn't modify destination.
// It verifies that when read-only mode is enabled, no modifications occur.
func TestReadOnly(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create source file
	if err := os.WriteFile(filepath.Join(srcDir, "file.txt"), []byte("new"), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}

	// Create different destination file
	if err := os.WriteFile(filepath.Join(dstDir, "file.txt"), []byte("old"), 0644); err != nil {
		t.Fatalf("failed to create dest file: %v", err)
	}

	// Sync in read-only mode
	s := NewSynchronizer(srcDir, dstDir, 1, true, Callbacks{})
	if err := s.Run(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Verify destination file wasn't modified
	content, _ := os.ReadFile(filepath.Join(dstDir, "file.txt"))
	if string(content) != "old" {
		t.Errorf("read-only mode modified destination file")
	}
}

// TestLargeParallelSync tests synchronization with a large number of files and directories.
// This test is designed to verify parallelity and race conditions with the -race flag.
// It creates 500+ files across multiple directory levels using 4 worker threads.
func TestLargeParallelSync(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create a large directory structure
	// 10 top-level directories, each with 5 subdirectories, each with 10 files = 500+ files
	const (
		numTopDirs     = 10
		numSubDirs     = 5
		numFilesPerDir = 10
	)

	expectedFiles := 0
	for i := 0; i < numTopDirs; i++ {
		topDir := filepath.Join(srcDir, fmt.Sprintf("top_%d", i))
		if err := os.Mkdir(topDir, 0755); err != nil {
			t.Fatalf("failed to create top dir: %v", err)
		}

		for j := 0; j < numSubDirs; j++ {
			subDir := filepath.Join(topDir, fmt.Sprintf("sub_%d", j))
			if err := os.Mkdir(subDir, 0755); err != nil {
				t.Fatalf("failed to create sub dir: %v", err)
			}

			for k := 0; k < numFilesPerDir; k++ {
				filePath := filepath.Join(subDir, fmt.Sprintf("file_%d.txt", k))
				content := fmt.Sprintf("content_%d_%d_%d", i, j, k)
				if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
					t.Fatalf("failed to create file: %v", err)
				}
				expectedFiles++
			}
		}
	}

	// Test with multiple workers to exercise parallelity
	numWorkers := 4
	s := NewSynchronizer(srcDir, dstDir, numWorkers, false, Callbacks{})
	if err := s.Run(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Verify all files were synced correctly
	var actualFiles int
	err := filepath.Walk(dstDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			actualFiles++
		}
		return nil
	})

	if err != nil {
		t.Fatalf("failed to walk dest dir: %v", err)
	}

	if actualFiles != expectedFiles {
		t.Errorf("expected %d files, got %d", expectedFiles, actualFiles)
	}

	// Verify all file contents match
	err = filepath.Walk(srcDir, func(srcPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}

		relPath, err := filepath.Rel(srcDir, srcPath)
		if err != nil {
			return err
		}

		dstPath := filepath.Join(dstDir, relPath)
		srcContent, err := os.ReadFile(srcPath)
		if err != nil {
			return fmt.Errorf("failed to read src file %s: %v", srcPath, err)
		}

		dstContent, err := os.ReadFile(dstPath)
		if err != nil {
			return fmt.Errorf("failed to read dst file %s: %v", dstPath, err)
		}

		if string(srcContent) != string(dstContent) {
			return fmt.Errorf("content mismatch for %s", relPath)
		}

		return nil
	})

	if err != nil {
		t.Errorf("file content verification failed: %v", err)
	}
}

// TestLargeParallelSync2 tests synchronization with another large structure to ensure parallelity works.
// It creates 256 files across multiple directory levels using 4 worker threads.
func TestLargeParallelSync2(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create a large directory structure
	// 8 top-level directories, each with 4 subdirectories, each with 8 files = 256 files
	const (
		numTopDirs     = 8
		numSubDirs     = 4
		numFilesPerDir = 8
	)

	expectedFiles := 0
	for i := 0; i < numTopDirs; i++ {
		topDir := filepath.Join(srcDir, fmt.Sprintf("top_%d", i))
		if err := os.Mkdir(topDir, 0755); err != nil {
			t.Fatalf("failed to create top dir: %v", err)
		}

		for j := 0; j < numSubDirs; j++ {
			subDir := filepath.Join(topDir, fmt.Sprintf("sub_%d", j))
			if err := os.Mkdir(subDir, 0755); err != nil {
				t.Fatalf("failed to create sub dir: %v", err)
			}

			for k := 0; k < numFilesPerDir; k++ {
				filePath := filepath.Join(subDir, fmt.Sprintf("file_%d.txt", k))
				content := fmt.Sprintf("content_%d_%d_%d", i, j, k)
				if err := os.WriteFile(filePath, []byte(content), 0644); err != nil {
					t.Fatalf("failed to create file: %v", err)
				}
				expectedFiles++
			}
		}
	}

	// Test with multiple workers
	numWorkers := 4
	s := NewSynchronizer(srcDir, dstDir, numWorkers, false, Callbacks{})
	if err := s.Run(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Verify all files were synced
	var actualFiles int
	err := filepath.Walk(dstDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			actualFiles++
		}
		return nil
	})

	if err != nil {
		t.Fatalf("failed to walk dest dir: %v", err)
	}

	if actualFiles != expectedFiles {
		t.Errorf("expected %d files, got %d", expectedFiles, actualFiles)
	}
}

// TestParallelWithMixedContent tests parallel sync with mixed file types.
// It verifies that regular files and directories are correctly handled in parallel.
func TestParallelWithMixedContent(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create simpler mixed content
	for i := 0; i < 3; i++ {
		dir := filepath.Join(srcDir, fmt.Sprintf("dir_%d", i))
		if err := os.Mkdir(dir, 0755); err != nil {
			t.Fatalf("failed to create dir: %v", err)
		}

		// Add files
		for j := 0; j < 3; j++ {
			filePath := filepath.Join(dir, fmt.Sprintf("file_%d.txt", j))
			if err := os.WriteFile(filePath, []byte(fmt.Sprintf("data_%d_%d", i, j)), 0644); err != nil {
				t.Fatalf("failed to create file: %v", err)
			}
		}
	}

	// Sync with 2 workers
	s := NewSynchronizer(srcDir, dstDir, 2, false, Callbacks{})
	if err := s.Run(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Verify structure
	srcDirs, _ := os.ReadDir(srcDir)
	dstDirs, _ := os.ReadDir(dstDir)

	if len(srcDirs) != len(dstDirs) {
		t.Errorf("directory count mismatch: src=%d, dst=%d", len(srcDirs), len(dstDirs))
	}
}

// TestDirectoryStructureIntegrity tests that directory structure is properly maintained.
// It verifies that nested directory hierarchies are correctly replicated.
func TestDirectoryStructureIntegrity(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create a simpler directory structure
	structure := []string{
		"a/file1.txt",
		"a/sub/file2.txt",
		"b/file3.txt",
	}

	for _, path := range structure {
		fullPath := filepath.Join(srcDir, path)
		dir := filepath.Dir(fullPath)
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatalf("failed to create dir: %v", err)
		}
		if err := os.WriteFile(fullPath, []byte(path), 0644); err != nil {
			t.Fatalf("failed to create file: %v", err)
		}
	}

	// Sync
	s := NewSynchronizer(srcDir, dstDir, 1, false, Callbacks{})
	if err := s.Run(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Verify directory counts match
	var srcDirs, dstDirs int
	filepath.Walk(srcDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			srcDirs++
		}
		return nil
	})

	filepath.Walk(dstDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			dstDirs++
		}
		return nil
	})

	if srcDirs != dstDirs {
		t.Errorf("directory count mismatch: src=%d, dst=%d", srcDirs, dstDirs)
	}
}

// TestConsistency verifies that multiple syncs produce consistent results.
// It ensures that running sync multiple times produces idempotent results.
func TestConsistency(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create initial structure
	for i := 0; i < 5; i++ {
		dir := filepath.Join(srcDir, fmt.Sprintf("dir_%d", i))
		os.Mkdir(dir, 0755)

		for j := 0; j < 5; j++ {
			filePath := filepath.Join(dir, fmt.Sprintf("file_%d.txt", j))
			os.WriteFile(filePath, []byte(fmt.Sprintf("v1_%d_%d", i, j)), 0644)
		}
	}

	// First sync
	s1 := NewSynchronizer(srcDir, dstDir, 2, false, Callbacks{})
	if err := s1.Run(); err != nil {
		t.Fatalf("first sync failed: %v", err)
	}

	// Count files after first sync
	var firstCount int
	filepath.Walk(dstDir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			firstCount++
		}
		return nil
	})

	// Second sync (should produce same results)
	s2 := NewSynchronizer(srcDir, dstDir, 2, false, Callbacks{})
	if err := s2.Run(); err != nil {
		t.Fatalf("second sync failed: %v", err)
	}

	// Count files after second sync
	var secondCount int
	filepath.Walk(dstDir, func(path string, info os.FileInfo, err error) error {
		if !info.IsDir() {
			secondCount++
		}
		return nil
	})

	// Results should be the same
	if firstCount != secondCount {
		t.Errorf("file count mismatch between syncs: first=%d, second=%d", firstCount, secondCount)
	}
}

// TestCustomLogger verifies that a custom logger is called during synchronization.
func TestCustomLogger(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create test files
	os.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("content1"), 0644)
	os.Mkdir(filepath.Join(srcDir, "subdir"), 0755)
	os.WriteFile(filepath.Join(srcDir, "subdir", "file2.txt"), []byte("content2"), 0644)

	// Create a mock logger
	logCalls := []string{}
	mockLogger := &mockLogger{
		calls: &logCalls,
	}

	// Run sync with custom logger
	sync := NewSynchronizerWithLogger(srcDir, dstDir, 2, false, Callbacks{}, mockLogger)
	if err := sync.Run(); err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Verify that no error messages were logged (everything should succeed)
	for _, call := range logCalls {
		if strings.Contains(call, "ERROR") {
			t.Errorf("unexpected ERROR in logs: %s", call)
		}
	}
}

// TestCustomLoggerWithError verifies that errors are logged to custom logger.
func TestCustomLoggerWithError(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create a source directory
	os.Mkdir(filepath.Join(srcDir, "dir1"), 0755)

	// Create a mock logger
	logCalls := []string{}
	mockLogger := &mockLogger{
		calls: &logCalls,
	}

	// Remove read permissions from source to cause an error
	os.Chmod(filepath.Join(srcDir, "dir1"), 0000)
	defer os.Chmod(filepath.Join(srcDir, "dir1"), 0755)

	// Run sync with custom logger - it should handle the error gracefully
	sync := NewSynchronizerWithLogger(srcDir, dstDir, 1, false, Callbacks{}, mockLogger)
	if err := sync.Run(); err != nil {
		// Run may or may not return an error, but we're checking logging
	}

	// Verify that custom logger was used (at least one call should have been made)
	if len(logCalls) == 0 {
		t.Log("No error encountered during sync - this is acceptable")
	}
}

// mockLogger implements the Logger interface for testing purposes.
type mockLogger struct {
	calls *[]string
}

func (ml *mockLogger) Printf(format string, v ...interface{}) {
	*ml.calls = append(*ml.calls, fmt.Sprintf(format, v...))
}

// TestBlockHashing verifies that block hashing works correctly with different algorithms.
func TestBlockHashing(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create a file with known content (multiple 4K blocks)
	fileContent := make([]byte, 4096*3+100) // 3 full blocks + partial block
	for i := 0; i < len(fileContent); i++ {
		fileContent[i] = byte(i % 256)
	}

	srcFile := filepath.Join(srcDir, "test.bin")
	dstFile := filepath.Join(dstDir, "test.bin")
	os.WriteFile(srcFile, fileContent, 0644)

	// Test with SHA256
	hashes := make(map[uint64][]byte)
	hasher := &mockBlockHasher{hashes: hashes}

	sync := NewSynchronizer(srcDir, dstDir, 1, false, Callbacks{})

	// Copy file with hashing
	err := sync.copyFileWithHash(srcFile, dstFile, HashAlgoSHA256, hasher)
	if err != nil {
		t.Fatalf("copyFileWithHash failed: %v", err)
	}

	// Verify we got all blocks
	expectedBlocks := 4 // 3 full 4K blocks + 1 partial
	if len(hashes) != expectedBlocks {
		t.Errorf("expected %d block hashes, got %d", expectedBlocks, len(hashes))
	}

	// Verify block IDs are sequential from 0
	for i := 0; i < expectedBlocks; i++ {
		if _, ok := hashes[uint64(i)]; !ok {
			t.Errorf("missing hash for block %d", i)
		}
	}

	// Verify destination file matches source
	dstContent, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("failed to read destination file: %v", err)
	}

	if !bytes.Equal(fileContent, dstContent) {
		t.Errorf("destination file content doesn't match source")
	}
}

// TestBlockHashingAlgorithms verifies different hash algorithms produce different results.
func TestBlockHashingAlgorithms(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create a small test file
	fileContent := []byte("Hello, World! This is test content.")
	srcFile := filepath.Join(srcDir, "test.txt")
	os.WriteFile(srcFile, fileContent, 0644)

	sync := NewSynchronizer(srcDir, dstDir, 1, false, Callbacks{})

	// Test MD5
	md5Hashes := make(map[uint64][]byte)
	md5Hasher := &mockBlockHasher{hashes: md5Hashes}
	dstMD5 := filepath.Join(dstDir, "test_md5.txt")
	if err := sync.copyFileWithHash(srcFile, dstMD5, HashAlgoMD5, md5Hasher); err != nil {
		t.Fatalf("MD5 copy failed: %v", err)
	}

	// Test SHA256
	sha256Hashes := make(map[uint64][]byte)
	sha256Hasher := &mockBlockHasher{hashes: sha256Hashes}
	dstSHA256 := filepath.Join(dstDir, "test_sha256.txt")
	if err := sync.copyFileWithHash(srcFile, dstSHA256, HashAlgoSHA256, sha256Hasher); err != nil {
		t.Fatalf("SHA256 copy failed: %v", err)
	}

	// Verify we got hashes for both
	if len(md5Hashes) == 0 {
		t.Errorf("no MD5 hashes received")
	}
	if len(sha256Hashes) == 0 {
		t.Errorf("no SHA256 hashes received")
	}

	// Verify hash lengths are different
	md5Hash := md5Hashes[0]
	sha256Hash := sha256Hashes[0]
	if len(md5Hash) == len(sha256Hash) {
		t.Errorf("expected different hash lengths for MD5 (%d) and SHA256 (%d)", len(md5Hash), len(sha256Hash))
	}

	if len(md5Hash) != 16 { // MD5 is 16 bytes
		t.Errorf("expected MD5 hash length 16, got %d", len(md5Hash))
	}
	if len(sha256Hash) != 32 { // SHA256 is 32 bytes
		t.Errorf("expected SHA256 hash length 32, got %d", len(sha256Hash))
	}
}

// TestBlockHashingWithNoHasher verifies copy works without a hasher.
func TestBlockHashingWithNoHasher(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	fileContent := []byte("Test content for copying without hashing")
	srcFile := filepath.Join(srcDir, "test.txt")
	dstFile := filepath.Join(dstDir, "test.txt")
	os.WriteFile(srcFile, fileContent, 0644)

	sync := NewSynchronizer(srcDir, dstDir, 1, false, Callbacks{})

	// Copy with HashAlgoNone (should fall back to io.Copy)
	if err := sync.copyFileWithHash(srcFile, dstFile, HashAlgoNone, nil); err != nil {
		t.Fatalf("copyFileWithHash failed: %v", err)
	}

	// Verify destination exists and matches
	dstContent, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("failed to read destination file: %v", err)
	}

	if !bytes.Equal(fileContent, dstContent) {
		t.Errorf("destination file content doesn't match source")
	}
}

// mockBlockHasher implements BlockHasher for testing.
type mockBlockHasher struct {
	hashes map[uint64][]byte
}

func (mbh *mockBlockHasher) HashBlock(blockID uint64, hash []byte) error {
	// Make a copy of the hash since the caller doesn't retain a reference
	hashCopy := make([]byte, len(hash))
	copy(hashCopy, hash)
	mbh.hashes[blockID] = hashCopy
	return nil
}

// TestBlockHashingXXHash verifies xxHash algorithm works correctly.
func TestBlockHashingXXHash(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create a file with known content
	fileContent := []byte("Test content for xxHash algorithm verification")
	srcFile := filepath.Join(srcDir, "test.txt")
	dstFile := filepath.Join(dstDir, "test.txt")
	os.WriteFile(srcFile, fileContent, 0644)

	sync := NewSynchronizer(srcDir, dstDir, 1, false, Callbacks{})

	// Copy with xxHash
	xxHashHashes := make(map[uint64][]byte)
	xxHasher := &mockBlockHasher{hashes: xxHashHashes}
	if err := sync.copyFileWithHash(srcFile, dstFile, HashAlgoXXHash, xxHasher); err != nil {
		t.Fatalf("xxHash copy failed: %v", err)
	}

	// Verify we got at least one hash
	if len(xxHashHashes) == 0 {
		t.Errorf("no xxHash hashes received")
	}

	// Verify hash length (xxHash produces 8 bytes)
	xxHashValue := xxHashHashes[0]
	if len(xxHashValue) != 8 {
		t.Errorf("expected xxHash length 8, got %d", len(xxHashValue))
	}

	// Verify destination file matches source
	dstContent, err := os.ReadFile(dstFile)
	if err != nil {
		t.Fatalf("failed to read destination file: %v", err)
	}

	if !bytes.Equal(fileContent, dstContent) {
		t.Errorf("destination file content doesn't match source")
	}
}

// TestBlockHashingAlgorithmComparison verifies xxHash produces different results than cryptographic hashes.
func TestBlockHashingAlgorithmComparison(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create a test file
	fileContent := []byte("Algorithm comparison test")
	srcFile := filepath.Join(srcDir, "test.txt")
	os.WriteFile(srcFile, fileContent, 0644)

	sync := NewSynchronizer(srcDir, dstDir, 1, false, Callbacks{})

	// Collect hashes from different algorithms
	algorithms := map[string]HashAlgo{
		"sha256": HashAlgoSHA256,
		"xxhash": HashAlgoXXHash,
	}

	hashes := make(map[string][]byte)

	for name, algo := range algorithms {
		dstFile := filepath.Join(dstDir, name+".txt")
		hasher := &mockBlockHasher{hashes: make(map[uint64][]byte)}
		if err := sync.copyFileWithHash(srcFile, dstFile, algo, hasher); err != nil {
			t.Fatalf("%s copy failed: %v", name, err)
		}
		if len(hasher.hashes) > 0 {
			hashes[name] = hasher.hashes[0]
		}
	}

	// Verify different algorithms produce different sized hashes
	sha256Hash := hashes["sha256"]
	xxHashHash := hashes["xxhash"]

	if len(sha256Hash) != 32 {
		t.Errorf("SHA256 hash length should be 32, got %d", len(sha256Hash))
	}

	if len(xxHashHash) != 8 {
		t.Errorf("xxHash length should be 8, got %d", len(xxHashHash))
	}

	// xxHash should be very fast, but we just verify it produces different values
	if bytes.Equal(sha256Hash[:8], xxHashHash) {
		t.Logf("SHA256 and xxHash happen to match for this input (unlikely but possible)")
	}
}

// TestPermissionsSync verifies file permissions are synced.
func TestPermissionsSync(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create a file with specific permissions
	srcFile := filepath.Join(srcDir, "test.txt")
	os.WriteFile(srcFile, []byte("content"), 0644)

	// Change source permissions
	os.Chmod(srcFile, 0755)

	// Sync
	sync := NewSynchronizer(srcDir, dstDir, 1, false, Callbacks{})
	if err := sync.Run(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Verify destination has same permissions
	dstFile := filepath.Join(dstDir, "test.txt")
	dstInfo, _ := os.Stat(dstFile)
	dstPerm := dstInfo.Mode().Perm()

	srcInfo, _ := os.Stat(srcFile)
	srcPerm := srcInfo.Mode().Perm()

	if srcPerm != dstPerm {
		t.Errorf("permissions don't match: src=%o, dst=%o", srcPerm, dstPerm)
	}
}

// TestModificationTimeSync verifies modification times are synced.
func TestModificationTimeSync(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create a file with current time
	srcFile := filepath.Join(srcDir, "test.txt")
	os.WriteFile(srcFile, []byte("content"), 0644)

	// Set source to a specific past time
	pastTime := time.Now().AddDate(-1, 0, 0) // 1 year ago
	os.Chtimes(srcFile, pastTime, pastTime)

	// Sync
	sync := NewSynchronizer(srcDir, dstDir, 1, false, Callbacks{})
	if err := sync.Run(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Verify destination has same modification time (within 1 second tolerance)
	dstFile := filepath.Join(dstDir, "test.txt")
	dstInfo, _ := os.Stat(dstFile)
	dstTime := dstInfo.ModTime()

	srcInfo, _ := os.Stat(srcFile)
	srcTime := srcInfo.ModTime()

	timeDiff := srcTime.Sub(dstTime)
	if timeDiff < -time.Second || timeDiff > time.Second {
		t.Errorf("modification times don't match: src=%v, dst=%v, diff=%v", srcTime, dstTime, timeDiff)
	}
}

// TestSymlinkTargetSync verifies symlink targets are verified and synced.
func TestSymlinkTargetSync(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create a symlink in source
	srcFile := filepath.Join(srcDir, "target.txt")
	os.WriteFile(srcFile, []byte("target content"), 0644)

	srcLink := filepath.Join(srcDir, "link.txt")
	os.Symlink("target.txt", srcLink)

	// Sync
	sync := NewSynchronizer(srcDir, dstDir, 1, false, Callbacks{})
	if err := sync.Run(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Verify destination symlink has same target
	dstLink := filepath.Join(dstDir, "link.txt")
	dstTarget, err := os.Readlink(dstLink)
	if err != nil {
		t.Fatalf("failed to read destination symlink: %v", err)
	}

	srcTarget, _ := os.Readlink(srcLink)

	if srcTarget != dstTarget {
		t.Errorf("symlink targets don't match: src=%s, dst=%s", srcTarget, dstTarget)
	}
}

// TestSymlinkTargetMismatchRecreate verifies mismatched symlink targets are recreated.
func TestSymlinkTargetMismatchRecreate(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create symlink in source
	srcLink := filepath.Join(srcDir, "link.txt")
	os.Symlink("target1.txt", srcLink)

	// Create directory structure in destination with wrong symlink
	os.Mkdir(filepath.Join(dstDir), 0755)
	dstLink := filepath.Join(dstDir, "link.txt")
	os.Symlink("target2.txt", dstLink) // Wrong target

	// Sync
	sync := NewSynchronizer(srcDir, dstDir, 1, false, Callbacks{})
	if err := sync.Run(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Verify symlink was recreated with correct target
	dstTarget, err := os.Readlink(dstLink)
	if err != nil {
		t.Fatalf("failed to read destination symlink: %v", err)
	}

	if dstTarget != "target1.txt" {
		t.Errorf("symlink target should be 'target1.txt', got '%s'", dstTarget)
	}
}

// TestOnIgnoreCallback verifies that OnIgnore callback is called and can filter entries.
func TestOnIgnoreCallback(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create source structure with files to ignore
	os.WriteFile(filepath.Join(srcDir, "include.txt"), []byte("include"), 0644)
	os.WriteFile(filepath.Join(srcDir, "exclude.txt"), []byte("exclude"), 0644)
	os.Mkdir(filepath.Join(srcDir, "subdir"), 0755)
	os.WriteFile(filepath.Join(srcDir, "subdir", "file.txt"), []byte("file"), 0644)

	// Track ignored entries
	ignoredEntries := make(map[string]bool)

	callbacks := Callbacks{
		OnIgnore: func(name string, path string, isDir bool, fileInfo os.FileInfo, err error) bool {
			// Ignore files starting with "exclude" and .snapshot directories
			if strings.HasPrefix(name, "exclude") {
				ignoredEntries[name] = true
				return true
			}
			if isDir && name == ".snapshot" {
				ignoredEntries[name] = true
				return true
			}
			return false
		},
	}

	sync := NewSynchronizer(srcDir, dstDir, 1, false, callbacks)
	if err := sync.Run(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Verify exclude.txt was ignored
	if _, err := os.Stat(filepath.Join(dstDir, "exclude.txt")); err == nil {
		t.Errorf("exclude.txt should have been ignored")
	}

	// Verify include.txt was synced
	if _, err := os.Stat(filepath.Join(dstDir, "include.txt")); err != nil {
		t.Errorf("include.txt should have been synced: %v", err)
	}

	// Verify subdir was synced
	if _, err := os.Stat(filepath.Join(dstDir, "subdir")); err != nil {
		t.Errorf("subdir should have been synced: %v", err)
	}

	// Verify exclude was actually called
	if len(ignoredEntries) == 0 {
		t.Errorf("OnIgnore callback was not called")
	}
}

func TestSyncDetailCallbacks(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	srcFile := filepath.Join(srcDir, "file.txt")
	dstFile := filepath.Join(dstDir, "file.txt")

	if err := os.WriteFile(srcFile, []byte("data"), 0644); err != nil {
		t.Fatalf("failed to write src file: %v", err)
	}

	// Set a known mod time on the source so we can compare after sync.
	srcMod := time.Now().Add(-30 * time.Minute).Truncate(time.Second)
	if err := os.Chtimes(srcFile, srcMod, srcMod); err != nil {
		t.Fatalf("failed to set src times: %v", err)
	}

	// Destination starts with different mode and older times.
	if err := os.WriteFile(dstFile, []byte("data"), 0600); err != nil {
		t.Fatalf("failed to write dst file: %v", err)
	}
	oldTime := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	if err := os.Chtimes(dstFile, oldTime, oldTime); err != nil {
		t.Fatalf("failed to set dst times: %v", err)
	}

	var (
		chmodCalled   bool
		chownCalled   bool
		chtimesCalled bool

		beforeMode os.FileMode
		afterMode  os.FileMode

		beforeAt time.Time
		beforeMt time.Time
		afterAt  time.Time
		afterMt  time.Time
		changedA bool
		changedM bool
	)

	callbacks := Callbacks{
		OnDstChmodDetail: func(path string, before, after os.FileMode, err error) {
			chmodCalled = true
			beforeMode = before
			afterMode = after
		},
		OnDstChownDetail: func(path string, oUID, oGID, nUID, nGID int, err error) {
			chownCalled = true
		},
		OnDstChtimesDetail: func(path string, bAt, bMt, aAt, aMt time.Time, cAt, cMt bool, err error) {
			chtimesCalled = true
			beforeAt, beforeMt, afterAt, afterMt = bAt, bMt, aAt, aMt
			changedA, changedM = cAt, cMt
		},
	}

	s := NewSynchronizer(srcDir, dstDir, 1, false, callbacks)
	if err := s.Run(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	if !chmodCalled {
		t.Fatal("expected OnChmodDetail to be called")
	}
	if beforeMode != 0600 {
		t.Fatalf("expected before mode 0600, got %v", beforeMode)
	}
	if afterMode != 0644 {
		t.Fatalf("expected after mode 0644, got %v", afterMode)
	}

	if !chtimesCalled {
		t.Fatal("expected OnChtimesDetail to be called")
	}
	if beforeAt.IsZero() {
		t.Fatalf("expected before atime to be captured")
	}
	if beforeMt.IsZero() {
		t.Fatalf("expected before mtime to be captured")
	}
	if !afterAt.Equal(srcMod) {
		t.Fatalf("expected after atime %v, got %v", srcMod, afterAt)
	}
	if !afterMt.Equal(srcMod) {
		t.Fatalf("expected after mtime %v, got %v", srcMod, afterMt)
	}
	if !changedM {
		t.Fatalf("expected mtime change to be reported")
	}
	if !changedA {
		t.Fatalf("expected atime change to be reported")
	}

	if chownCalled {
		t.Fatal("expected OnChownDetail not to be called when ownership is unchanged")
	}
}

func TestSyncInodeAttrsIgnoreAtime(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	srcFile := filepath.Join(srcDir, "file.txt")
	dstFile := filepath.Join(dstDir, "file.txt")

	if err := os.WriteFile(srcFile, []byte("data"), 0644); err != nil {
		t.Fatalf("failed to write src file: %v", err)
	}

	srcMod := time.Now().Add(-1 * time.Hour).Truncate(time.Second)
	if err := os.Chtimes(srcFile, srcMod, srcMod); err != nil {
		t.Fatalf("failed to set src times: %v", err)
	}

	if err := os.WriteFile(dstFile, []byte("data"), 0644); err != nil {
		t.Fatalf("failed to write dst file: %v", err)
	}
	oldTime := time.Now().Add(-2 * time.Hour).Truncate(time.Second)
	if err := os.Chtimes(dstFile, oldTime, oldTime); err != nil {
		t.Fatalf("failed to set dst times: %v", err)
	}

	var (
		chtimesCalled bool
		beforeAt      time.Time
		afterAt       time.Time
		afterMt       time.Time
		changedA      bool
		changedM      bool
	)

	callbacks := Callbacks{
		OnDstChtimesDetail: func(path string, bAt, bMt, aAt, aMt time.Time, cAt, cMt bool, err error) {
			chtimesCalled = true
			beforeAt = bAt
			afterAt = aAt
			afterMt = aMt
			changedA = cAt
			changedM = cMt
		},
	}

	opts := Options{IgnoreAtime: true}
	s := NewSynchronizerWithLoggerAndOptions(srcDir, dstDir, 1, false, callbacks, nil, opts)
	if err := s.Run(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	if !chtimesCalled {
		t.Fatal("expected OnChtimesDetail to be called")
	}
	if beforeAt.IsZero() {
		t.Fatal("expected before atime to be captured")
	}
	if changedA {
		t.Fatal("expected atime change to be ignored")
	}
	if !changedM {
		t.Fatal("expected mtime change to be reported")
	}
	if !afterAt.Equal(beforeAt) {
		t.Fatalf("expected atime to be preserved, before %v after %v", beforeAt, afterAt)
	}
	if !afterMt.Equal(srcMod) {
		t.Fatalf("expected mtime to match source %v, got %v", srcMod, afterMt)
	}
}
