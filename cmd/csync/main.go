// Command csync provides a CLI wrapper around the csync library for syncing one
// directory tree into another with optional verbose logs and periodic stats.
package main

import (
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jedib0t/go-pretty/v6/table"
	"github.com/jedib0t/go-pretty/v6/text"
	"github.com/spf13/cobra"

	"csync"
)

// statsCollector aggregates operation counts and byte totals using atomic operations.
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

// statsSnapshot captures a point-in-time view of collected statistics.
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

// snapshot returns a consistent view of current stats at a given moment.
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
	var logOpsFlag []string
	var noLogOpsFlag []string
	var ignoreAtime bool
	var showWorkers bool

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

			if showWorkers {
				fmt.Printf("workers: cpus=%d max-workers=%d\n", runtime.NumCPU(), workers)
			}

			excludeSet := make(map[string]struct{}, len(excludes))
			for _, name := range excludes {
				excludeSet[name] = struct{}{}
			}

			stats := &statsCollector{}
			start := time.Now()

			logOps := map[string]bool{
				"mkdir":     true,
				"unlink":    true,
				"removeall": true,
				"symlink":   true,
				"chmod":     true,
				"chown":     true,
				"chtimes":   true,
				"copy":      true,
			}
			applyOps := func(list []string, val bool) {
				for _, entry := range list {
					for _, op := range strings.Split(entry, ",") {
						op = strings.ToLower(strings.TrimSpace(op))
						if op == "" {
							continue
						}
						logOps[op] = val
					}
				}
			}
			applyOps(logOpsFlag, true)
			applyOps(noLogOpsFlag, false)
			shouldLog := func(op string) bool {
				if !verbose {
					return false
				}
				v, ok := logOps[strings.ToLower(op)]
				return ok && v
			}

			callbacks := csync.Callbacks{
				OnIgnore: func(name string, path string, isDir bool, fileInfo os.FileInfo, err error) bool {
					if err != nil {
						return false
					}
					if _, ok := excludeSet[name]; ok {
						if shouldLog("ignore") {
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
					if shouldLog("lstat") {
						logMsg("lstat", path, err)
					}
				},
				OnReadDir: func(path string, entries []os.DirEntry, err error) {
					if err == nil {
						stats.readdir.Add(1)
					}
					if shouldLog("readdir") {
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
					if shouldLog("copy") {
						logMsg("copy", fmt.Sprintf("%s -> %s (%s)", srcPath, dstPath, formatBytes(uint64(size))), err)
					}
				},
				OnMkdir: func(path string, mode os.FileMode, err error) {
					if err == nil {
						stats.mkdir.Add(1)
					}
					if shouldLog("mkdir") {
						logMsg("mkdir", fmt.Sprintf("%s (%s)", path, mode.String()), err)
					}
				},
				OnUnlink: func(path string, err error) {
					if err == nil {
						stats.unlink.Add(1)
					}
					if shouldLog("unlink") {
						logMsg("unlink", path, err)
					}
				},
				OnRemoveAll: func(path string, err error) {
					if err == nil {
						stats.removeAll.Add(1)
					}
					if shouldLog("removeall") {
						logMsg("removeall", path, err)
					}
				},
				OnSymlink: func(linkPath, target string, err error) {
					if err == nil {
						stats.symlink.Add(1)
					}
					if shouldLog("symlink") {
						logMsg("symlink", fmt.Sprintf("%s -> %s", linkPath, target), err)
					}
				},
				OnChmod: func(path string, mode os.FileMode, err error) {
					if err == nil {
						stats.chmod.Add(1)
					}
					if shouldLog("chmod") {
						logMsg("chmod", fmt.Sprintf("%s -> %s", path, mode.String()), err)
					}
				},
				OnChown: func(path string, uid, gid int, err error) {
					if err == nil {
						stats.chown.Add(1)
					}
					if shouldLog("chown") {
						logMsg("chown", fmt.Sprintf("%s -> %d:%d", path, uid, gid), err)
					}
				},
				OnChtimes: func(path string, err error) {
					if err == nil {
						stats.chtimes.Add(1)
					}
					if shouldLog("chtimes") {
						logMsg("chtimes", path, err)
					}
				},
				OnChmodDetail: func(path string, before, after os.FileMode, err error) {
					if !shouldLog("chmod") {
						return
					}
					logMsg("chmod", fmt.Sprintf("%s: %s -> %s", path, before.String(), after.String()), err)
				},
				OnChownDetail: func(path string, oldUID, oldGID, newUID, newGID int, err error) {
					if !shouldLog("chown") {
						return
					}
					logMsg("chown", fmt.Sprintf("%s: %d:%d -> %d:%d", path, oldUID, oldGID, newUID, newGID), err)
				},
				OnChtimesDetail: func(path string, beforeAtime, beforeMtime, afterAtime, afterMtime time.Time, changedAtime, changedMtime bool, err error) {
					if !shouldLog("chtimes") {
						return
					}
					var changed []string
					if changedAtime {
						changed = append(changed, fmt.Sprintf("atime %s -> %s", beforeAtime.Format(time.RFC3339), afterAtime.Format(time.RFC3339)))
					}
					if changedMtime {
						changed = append(changed, fmt.Sprintf("mtime %s -> %s", beforeMtime.Format(time.RFC3339), afterMtime.Format(time.RFC3339)))
					}
					if len(changed) == 0 {
						logMsg("chtimes", fmt.Sprintf("%s: no time change", path), err)
						return
					}
					logMsg("chtimes", fmt.Sprintf("%s: %s", path, strings.Join(changed, ", ")), err)
				},
			}

			opts := csync.Options{IgnoreAtime: ignoreAtime}
			syncer := csync.NewSynchronizerWithLoggerAndOptions(src, dst, workers, false, callbacks, nil, opts)

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
	rootCmd.Flags().IntVar(&workers, "max-workers", 32, "maximum number of concurrent workers (>=1)")
	rootCmd.Flags().StringArrayVar(&excludes, "exclude", nil, "exclude entries whose base name matches (repeatable)")
	rootCmd.Flags().StringArrayVar(&logOpsFlag, "log-op", nil, "include additional operations in verbose output (comma-separated or repeatable)")
	rootCmd.Flags().StringArrayVar(&noLogOpsFlag, "no-log-op", nil, "exclude operations from verbose output (comma-separated or repeatable)")
	rootCmd.Flags().BoolVar(&ignoreAtime, "ignore-atime", false, "ignore atime differences when syncing times (preserve existing atime)")
	rootCmd.Flags().BoolVar(&showWorkers, "show-workers", false, "print detected CPUs and max worker count at start")

	if err := rootCmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// logMsg prints a timestamped log message with operation kind, details, and optional error.
func logMsg(kind string, details interface{}, err error) {
	if err != nil {
		fmt.Printf("[%s] %v (err=%v)\n", kind, details, err)
		return
	}
	fmt.Printf("[%s] %v\n", kind, details)
}

// runStatsPrinter runs a background goroutine that prints stats every 10 seconds and on completion.
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
			_ = now
			return
		}
	}
}

// printStatsTable formats and renders a stats table with operation counts and rates.
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

	var names []string
	var totals []string
	var avgs []string
	var intervals []string

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleRounded)
	t.Style().Color.Row = text.Colors{text.Reset}
	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 2, Align: text.AlignRight},
		{Number: 3, Align: text.AlignRight},
		{Number: 4, Align: text.AlignRight},
	})
	t.AppendHeader(table.Row{text.Bold.Sprint("Operation"), text.Bold.Sprint("Total"), text.Bold.Sprint("Avg/s"), text.Bold.Sprint("Avg/s (interval)")})

	for i, r := range rows {
		if r.total == 0 {
			continue
		}
		prevTotal := getPrevTotal(prev, r.name)
		totalRate := float64(r.total) / elapsed
		intervalRate := float64(r.total-prevTotal) / interval
		names = append(names, r.name)
		totals = append(totals, formatScaledUint(r.total, ""))
		avgs = append(avgs, formatScaledFloat(totalRate, "/s"))
		intervals = append(intervals, formatScaledFloat(intervalRate, "/s"))
	}

	prevBytes := prev.bytes
	bytesRateTotal := float64(cur.bytes) / elapsed
	bytesRateInterval := float64(cur.bytes-prevBytes) / interval
	if cur.bytes > 0 {
		names = append(names, "bytes")
		totals = append(totals, formatScaledUint(cur.bytes, "B"))
		avgs = append(avgs, formatScaledFloat(bytesRateTotal, "B/s"))
		intervals = append(intervals, formatScaledFloat(bytesRateInterval, "B/s"))
	}

	// If nothing recorded, still render headers for clarity.
	if len(names) == 0 {
		t.Render()
		return
	}

	totals = alignDecimal(totals)
	avgs = alignDecimal(avgs)
	intervals = alignDecimal(intervals)

	for i, name := range names {
		t.AppendRow(table.Row{
			text.Bold.Sprint(name),
			totals[i],
			avgs[i],
			intervals[i],
		})
	}

	t.Render()
}

// getPrevTotal returns the previous total for a named operation from a snapshot.
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

// formatScaledUint renders an integer using scaled units (k, m, g, t...) with one decimal place.
// Returns an empty string when the value is zero.
func formatScaledUint(n uint64, suffix string) string {
	return formatScaledFloat(float64(n), suffix)
}

// formatScaledFloat renders a float using scaled units (k, m, g, t...) with one decimal place.
// Returns an empty string when the value is zero.
func formatScaledFloat(v float64, suffix string) string {
	if v == 0 {
		return ""
	}
	units := []string{"", "k", "m", "g", "t", "p", "e"}
	idx := 0
	abs := v
	if abs < 0 {
		abs = -abs
	}
	for abs >= 1000 && idx < len(units)-1 {
		v /= 1000
		abs /= 1000
		idx++
	}
	return fmt.Sprintf("%.1f%s%s", v, units[idx], suffix)
}

// alignDecimal pads values so their decimal points line up in a column.
func alignDecimal(values []string) []string {
	maxInt := 0
	for _, v := range values {
		if v == "" {
			continue
		}
		dot := strings.IndexByte(v, '.')
		if dot == -1 {
			dot = len(v)
		}
		if dot > maxInt {
			maxInt = dot
		}
	}

	out := make([]string, len(values))
	for i, v := range values {
		if v == "" {
			out[i] = ""
			continue
		}
		dot := strings.IndexByte(v, '.')
		if dot == -1 {
			dot = len(v)
		}
		pad := maxInt - dot
		if pad > 0 {
			out[i] = strings.Repeat(" ", pad) + v
		} else {
			out[i] = v
		}
	}
	return out
}

// formatCount formats an unsigned integer with thousands separators.
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

// formatFloat formats a floating-point number to two decimal places.
func formatFloat(f float64) string {
	return fmt.Sprintf("%.2f", f)
}

// formatBytes formats a byte count as a human-readable string (B, KB, MB, GB, TB).
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

// formatBytesRate formats a transfer rate in bytes/sec as a human-readable string.
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
