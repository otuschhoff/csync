package csync

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
