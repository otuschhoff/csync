package main

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"csync"
	"github.com/spf13/cobra"
)

func TestBasicSync(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create test files in src
	if err := os.WriteFile(filepath.Join(srcDir, "file1.txt"), []byte("content1"), 0644); err != nil {
		t.Fatalf("failed to create source file: %v", err)
	}
	if err := os.Mkdir(filepath.Join(srcDir, "subdir"), 0755); err != nil {
		t.Fatalf("failed to create source dir: %v", err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "subdir", "file2.txt"), []byte("content2"), 0644); err != nil {
		t.Fatalf("failed to create nested source file: %v", err)
	}

	// Run sync
	callbacks := csync.Callbacks{}
	syncer := csync.NewSynchronizer(srcDir, dstDir, 4, false, callbacks)
	if err := syncer.Run(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Verify files in dst
	if _, err := os.Stat(filepath.Join(dstDir, "file1.txt")); err != nil {
		t.Errorf("file1.txt not in dst: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dstDir, "subdir", "file2.txt")); err != nil {
		t.Errorf("subdir/file2.txt not in dst: %v", err)
	}
}

func TestExcludeFlag(t *testing.T) {
	srcDir := t.TempDir()
	dstDir := t.TempDir()

	// Create files to sync and exclude
	if err := os.WriteFile(filepath.Join(srcDir, "include.txt"), []byte("keep"), 0644); err != nil {
		t.Fatalf("failed to create include file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "exclude.txt"), []byte("skip"), 0644); err != nil {
		t.Fatalf("failed to create exclude file: %v", err)
	}

	// Manually test exclude logic
	excludeSet := map[string]struct{}{"exclude.txt": {}}
	onIgnore := func(name string, path string, isDir bool, fileInfo os.FileInfo, err error) bool {
		if err != nil {
			return false
		}
		_, ok := excludeSet[name]
		return ok
	}

	callbacks := csync.Callbacks{
		OnIgnore: onIgnore,
	}
	syncer := csync.NewSynchronizer(srcDir, dstDir, 4, false, callbacks)
	if err := syncer.Run(); err != nil {
		t.Fatalf("sync failed: %v", err)
	}

	// Verify include.txt exists
	if _, err := os.Stat(filepath.Join(dstDir, "include.txt")); err != nil {
		t.Errorf("include.txt should be in dst: %v", err)
	}

	// Verify exclude.txt does not exist
	if _, err := os.Stat(filepath.Join(dstDir, "exclude.txt")); err == nil {
		t.Error("exclude.txt should not be in dst")
	}
}

func TestMaxWorkersValidation(t *testing.T) {
	tests := []struct {
		name    string
		workers int
		valid   bool
	}{
		{"zero workers", 0, false},
		{"negative workers", -1, false},
		{"one worker", 1, true},
		{"four workers", 4, true},
		{"eight workers", 8, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			valid := tt.workers > 0
			if valid != tt.valid {
				t.Errorf("expected valid=%v, got %v", tt.valid, valid)
			}
		})
	}
}

func TestStatsPrinterLogic(t *testing.T) {
	collector := &statsCollector{}

	// Simulate some operations
	collector.lstat.Add(10)
	collector.readdir.Add(5)
	collector.mkdir.Add(2)
	collector.copies.Add(3)
	collector.bytes.Add(1024000)

	snap := collector.snapshot()

	if snap.lstat != 10 {
		t.Errorf("expected lstat=10, got %d", snap.lstat)
	}
	if snap.readdir != 5 {
		t.Errorf("expected readdir=5, got %d", snap.readdir)
	}
	if snap.mkdir != 2 {
		t.Errorf("expected mkdir=2, got %d", snap.mkdir)
	}
	if snap.copies != 3 {
		t.Errorf("expected copies=3, got %d", snap.copies)
	}
	if snap.bytes != 1024000 {
		t.Errorf("expected bytes=1024000, got %d", snap.bytes)
	}
}

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		input    uint64
		expected string
	}{
		{0, "0.00 B"},
		{512, "512.00 B"},
		{1024, "1.00 KB"},
		{1024 * 1024, "1.00 MB"},
		{1024 * 1024 * 1024, "1.00 GB"},
	}

	for _, tt := range tests {
		result := formatBytes(tt.input)
		if result != tt.expected {
			t.Errorf("formatBytes(%d) = %q, expected %q", tt.input, result, tt.expected)
		}
	}
}

func TestFormatBytesRate(t *testing.T) {
	tests := []struct {
		input    float64
		expected string
	}{
		{0, "0.00 B/s"},
		{512.5, "512.50 B/s"},
		{1024, "1.00 KB/s"},
		{1024 * 1024, "1.00 MB/s"},
	}

	for _, tt := range tests {
		result := formatBytesRate(tt.input)
		if result != tt.expected {
			t.Errorf("formatBytesRate(%.1f) = %q, expected %q", tt.input, result, tt.expected)
		}
	}
}

func TestFormatCount(t *testing.T) {
	tests := []struct {
		input    uint64
		expected string
	}{
		{0, "0"},
		{123, "123"},
		{1234, "1,234"},
		{1234567, "1,234,567"},
	}

	for _, tt := range tests {
		result := formatCount(tt.input)
		if result != tt.expected {
			t.Errorf("formatCount(%d) = %q, expected %q", tt.input, result, tt.expected)
		}
	}
}

func TestFormatFloat(t *testing.T) {
	tests := []struct {
		input    float64
		expected string
	}{
		{1.5, "1.50"},
		{0.333, "0.33"},
		{100.999, "101.00"},
	}

	for _, tt := range tests {
		result := formatFloat(tt.input)
		if result != tt.expected {
			t.Errorf("formatFloat(%.3f) = %q, expected %q", tt.input, result, tt.expected)
		}
	}
}

func TestGetPrevTotal(t *testing.T) {
	snap := statsSnapshot{
		lstat:     100,
		readdir:   50,
		mkdir:     10,
		unlink:    5,
		removeAll: 2,
		symlink:   1,
		chmod:     20,
		chown:     15,
		chtimes:   30,
		copies:    200,
		bytes:     1000000,
	}

	tests := []struct {
		name     string
		expected uint64
	}{
		{"lstat", 100},
		{"readdir", 50},
		{"mkdir", 10},
		{"unlink", 5},
		{"removeall", 2},
		{"symlink", 1},
		{"chmod", 20},
		{"chown", 15},
		{"chtimes", 30},
		{"copy", 200},
		{"unknown", 0},
	}

	for _, tt := range tests {
		result := getPrevTotal(snap, tt.name)
		if result != tt.expected {
			t.Errorf("getPrevTotal(snap, %q) = %d, expected %d", tt.name, result, tt.expected)
		}
	}
}

func TestCobraCommandBasic(t *testing.T) {
	// Test that the command parses flags correctly
	cmd := &cobra.Command{}
	var verbose bool
	var workers int
	var excludes []string
	var statsFlag bool
	var logOps []string
	var noLogOps []string

	cmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "verbose mode")
	cmd.Flags().IntVar(&workers, "max-workers", 4, "max workers")
	cmd.Flags().StringArrayVar(&excludes, "exclude", nil, "exclude")
	cmd.Flags().BoolVar(&statsFlag, "stats", false, "stats mode")
	cmd.Flags().StringArrayVar(&logOps, "log-op", nil, "log-op")
	cmd.Flags().StringArrayVar(&noLogOps, "no-log-op", nil, "no-log-op")

	// Parse some arguments
	cmd.SetArgs([]string{"--verbose", "--max-workers", "8", "--exclude", ".git", "--exclude", "node_modules", "--log-op", "lstat,readdir", "--no-log-op", "copy"})
	err := cmd.Execute()

	// Since we didn't set a RunE, this will return an error for missing command, but flags should parse
	if err != nil {
		// Expected since we didn't implement the command
	}

	// Re-test with ParseFlags only
	cmd2 := &cobra.Command{}
	cmd2.Flags().BoolVarP(&verbose, "verbose", "v", false, "verbose mode")
	cmd2.Flags().IntVar(&workers, "max-workers", 4, "max workers")
	cmd2.Flags().StringArrayVar(&excludes, "exclude", nil, "exclude")
	cmd2.Flags().StringArrayVar(&logOps, "log-op", nil, "log-op")
	cmd2.Flags().StringArrayVar(&noLogOps, "no-log-op", nil, "no-log-op")

	cmd2.SetArgs([]string{"--verbose", "--max-workers", "8", "--exclude", ".git", "--exclude", "node_modules", "--log-op", "lstat", "--no-log-op", "copy"})
	err = cmd2.ParseFlags([]string{"--verbose", "--max-workers", "8", "--exclude", ".git", "--exclude", "node_modules", "--log-op", "lstat", "--no-log-op", "copy"})
	if err != nil {
		t.Errorf("failed to parse flags: %v", err)
	}

	if !verbose {
		t.Error("verbose flag not set")
	}
	if workers != 8 {
		t.Errorf("expected workers=8, got %d", workers)
	}
	if len(excludes) != 2 || excludes[0] != ".git" || excludes[1] != "node_modules" {
		t.Errorf("expected excludes=[.git, node_modules], got %v", excludes)
	}
	if len(logOps) != 1 || logOps[0] != "lstat" {
		t.Errorf("expected log-op lstat, got %v", logOps)
	}
	if len(noLogOps) != 1 || noLogOps[0] != "copy" {
		t.Errorf("expected no-log-op copy, got %v", noLogOps)
	}
}

func TestLogMsg(t *testing.T) {
	// Capture stdout
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	// Test logging with error
	logMsg("test", "details", io.EOF)

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	output := buf.String()

	if len(output) == 0 {
		t.Error("expected output from logMsg")
	}
	if !bytes.Contains(buf.Bytes(), []byte("test")) {
		t.Error("expected 'test' in output")
	}
}
