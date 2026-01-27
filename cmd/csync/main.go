// Command csync provides a CLI wrapper around the csync library for syncing one
// directory tree into another with optional verbose logs and periodic stats.
package main

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"

	"csync"
)

type statsCollector struct {
	lstat     atomic.Uint64
	readdir   atomic.Uint64
	mkdir     atomic.Uint64
	unlink    atomic.Uint64
	removeAll atomic.Uint64
	symlink   atomic.Uint64
	chmod     atomic.Uint64
	chown     atomic.Uint64
	chtimes   atomic.Uint64
	copies    atomic.Uint64
	bytes     atomic.Uint64
}

type statsSnapshot struct {
	lstat     uint64
	readdir   uint64
	mkdir     uint64
	unlink    uint64
	removeAll uint64
	symlink   uint64
	chmod     uint64
	chown     uint64
	chtimes   uint64
	copies    uint64
	bytes     uint64
}

func (s *statsCollector) snapshot() statsSnapshot {
	return statsSnapshot{
		lstat:     s.lstat.Load(),
		readdir:   s.readdir.Load(),
		mkdir:     s.mkdir.Load(),
		unlink:    s.unlink.Load(),
		removeAll: s.removeAll.Load(),
		symlink:   s.symlink.Load(),
		chmod:     s.chmod.Load(),
		chown:     s.chown.Load(),
		chtimes:   s.chtimes.Load(),
		copies:    s.copies.Load(),
		bytes:     s.bytes.Load(),
	}
}

func main() {
	var verbose bool
	var statsFlag bool
	var workers int
	var excludes []string

	rootCmd := &cobra.Command{
		Use:   "csync <src> <dst>",
		Short: "Synchronize directories with csync",
		Args: func(cmd *cobra.Command, args []string) error {
			if len(args) != 2 {
				return fmt.Errorf("expected <src> <dst>")
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			src := args[0]
			dst := args[1]
			if workers <= 0 {
				return fmt.Errorf("max-workers must be >= 1")
			}

			excludeSet := make(map[string]struct{}, len(excludes))
			for _, name := range excludes {
				excludeSet[name] = struct{}{}
			}

			stats := &statsCollector{}
			start := time.Now()

			callbacks := csync.Callbacks{
				OnIgnore: func(name string, path string, isDir bool, fileInfo os.FileInfo, err error) bool {
					if err != nil {
						return false
					}
					if _, ok := excludeSet[name]; ok {
						if verbose {
							logMsg("ignore", path, nil)
						}
						return true
					}
					return false
				},
				OnLstat: func(path string, isDir bool, fileInfo os.FileInfo, err error) {
					if err == nil {
						stats.lstat.Add(1)
					}
					if verbose {
						logMsg("lstat", path, err)
					}
				},
				OnReadDir: func(path string, entries []os.DirEntry, err error) {
					if err == nil {
						stats.readdir.Add(1)
					}
					if verbose {
						logMsg("readdir", fmt.Sprintf("%s (%d entries)", path, len(entries)), err)
					}
				},
				OnCopy: func(srcPath, dstPath string, size int64, err error) {
					if err == nil {
						stats.copies.Add(1)
						if size > 0 {
							stats.bytes.Add(uint64(size))
						}
					}
					if verbose {
						logMsg("copy", fmt.Sprintf("%s -> %s (%s)", srcPath, dstPath, formatBytes(uint64(size))), err)
					}
				},
				OnMkdir: func(path string, mode os.FileMode, err error) {
					if err == nil {
						stats.mkdir.Add(1)
					}
					if verbose {
						logMsg("mkdir", fmt.Sprintf("%s (%s)", path, mode.String()), err)
					}
				},
				OnUnlink: func(path string, err error) {
					if err == nil {
						stats.unlink.Add(1)
					}
					if verbose {
						logMsg("unlink", path, err)
					}
				},
				OnRemoveAll: func(path string, err error) {
					if err == nil {
						stats.removeAll.Add(1)
					}
					if verbose {
						logMsg("removeall", path, err)
					}
				},
				OnSymlink: func(linkPath, target string, err error) {
					if err == nil {
						stats.symlink.Add(1)
					}
					if verbose {
						logMsg("symlink", fmt.Sprintf("%s -> %s", linkPath, target), err)
					}
				},
				OnChmod: func(path string, mode os.FileMode, err error) {
					if err == nil {
						stats.chmod.Add(1)
					}
					if verbose {
						logMsg("chmod", fmt.Sprintf("%s (%s)", path, mode.String()), err)
					}
				},
				OnChown: func(path string, uid, gid int, err error) {
					if err == nil {
						stats.chown.Add(1)
					}
					if verbose {
						logMsg("chown", fmt.Sprintf("%s (%d:%d)", path, uid, gid), err)
					}
				},
				OnChtimes: func(path string, err error) {
					if err == nil {
						stats.chtimes.Add(1)
					}
					if verbose {
						logMsg("chtimes", path, err)
					}
				},
			}

			syncer := csync.NewSynchronizer(src, dst, workers, false, callbacks)

			var done chan struct{}
			if statsFlag {
				done = make(chan struct{})
				go runStatsPrinter(stats, done, start)
			}

			err := syncer.Run()
			if statsFlag {
				close(done)
			}
			return err
		},
	}

	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "print verbose operation logs")
	rootCmd.Flags().BoolVar(&statsFlag, "stats", false, "print stats every 10s during sync")
	rootCmd.Flags().IntVar(&workers, "max-workers", 4, "maximum number of concurrent workers (>=1)")
	rootCmd.Flags().StringArrayVar(&excludes, "exclude", nil, "exclude entries whose base name matches (repeatable)")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func logMsg(kind string, details interface{}, err error) {
	if err != nil {
		fmt.Printf("[%s] %v (err=%v)\n", kind, details, err)
		return
	}
	fmt.Printf("[%s] %v\n", kind, details)
}

func runStatsPrinter(stats *statsCollector, done <-chan struct{}, start time.Time) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	lastSnapshot := stats.snapshot()
	lastTime := start

	for {
		select {
		case <-ticker.C:
			now := time.Now()
			snapshot := stats.snapshot()
			printStatsTable(snapshot, lastSnapshot, start, lastTime)
			lastSnapshot = snapshot
			lastTime = now
		case <-done:
			now := time.Now()
			snapshot := stats.snapshot()
			printStatsTable(snapshot, lastSnapshot, start, lastTime)
			return
		}
	}
}

func printStatsTable(cur, prev statsSnapshot, start, prevTime time.Time) {
	elapsed := time.Since(start).Seconds()
	interval := time.Since(prevTime).Seconds()
	if elapsed == 0 {
		elapsed = 1
	}
	if interval == 0 {
		interval = 1
	}

	rows := []struct {
		name  string
		total uint64
	}{
		{"lstat", cur.lstat},
		{"readdir", cur.readdir},
		{"mkdir", cur.mkdir},
		{"unlink", cur.unlink},
		{"removeall", cur.removeAll},
		{"symlink", cur.symlink},
		{"chmod", cur.chmod},
		{"chown", cur.chown},
		{"chtimes", cur.chtimes},
		{"copy", cur.copies},
	}

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleRounded)
	t.Style().Color.Row = text.Colors{text.Reset}
	t.AppendHeader(table.Row{"Operation", "Total", "Avg/s", "Avg/s (interval)"})

	for _, r := range rows {
		prevTotal := getPrevTotal(prev, r.name)
		totalRate := float64(r.total) / elapsed
		intervalRate := float64(r.total-prevTotal) / interval
		t.AppendRow(table.Row{
			r.name,
			formatCount(r.total),
			formatFloat(totalRate),
			formatFloat(intervalRate),
		})
	}

	prevBytes := prev.bytes
	bytesRateTotal := float64(cur.bytes) / elapsed
	bytesRateInterval := float64(cur.bytes-prevBytes) / interval
	t.AppendRow(table.Row{"bytes", formatBytes(cur.bytes), formatBytesRate(bytesRateTotal), formatBytesRate(bytesRateInterval)})

	t.Render()
}

func getPrevTotal(prev statsSnapshot, name string) uint64 {
	switch name {
	case "lstat":
		return prev.lstat
	case "readdir":
		return prev.readdir
	case "mkdir":
		return prev.mkdir
	case "unlink":
		return prev.unlink
	case "removeall":
		return prev.removeAll
	case "symlink":
		return prev.symlink
	case "chmod":
		return prev.chmod
	case "chown":
		return prev.chown
	case "chtimes":
		return prev.chtimes
	case "copy":
		return prev.copies
	default:
		return 0
	}
}

func formatCount(n uint64) string {
	s := fmt.Sprintf("%d", n)
	if len(s) <= 3 {
		return s
	}
	var b strings.Builder
	rem := len(s) % 3
	if rem == 0 {
		rem = 3
	}
	b.WriteString(s[:rem])
	for i := rem; i < len(s); i += 3 {
		b.WriteString(",")
		b.WriteString(s[i : i+3])
	}
	return b.String()
}

func formatFloat(f float64) string {
	return fmt.Sprintf("%.2f", f)
}

func formatBytes(n uint64) string {
	units := []string{"B", "KB", "MB", "GB", "TB"}
	v := float64(n)
	idx := 0
	for v >= 1024 && idx < len(units)-1 {
		v /= 1024
		idx++
	}
	return fmt.Sprintf("%.2f %s", v, units[idx])
}

func formatBytesRate(rate float64) string {
	if rate < 0 {
		rate = 0
	}
	units := []string{"B/s", "KB/s", "MB/s", "GB/s", "TB/s"}
	v := rate
	idx := 0
	for v >= 1024 && idx < len(units)-1 {
		v /= 1024
		idx++
	}
	return fmt.Sprintf("%.2f %s", v, units[idx])
}
