// Command csync provides a CLI wrapper around the csync library for syncing one
// directory tree into another with optional verbose logs and periodic stats.
package main

import (
	"fmt"
	"os"
	"path/filepath"
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
	lstat             atomic.Uint64
	readdir           atomic.Uint64
	mkdir             atomic.Uint64
	unlink            atomic.Uint64
	removeAll         atomic.Uint64
	symlink           atomic.Uint64
	chmod             atomic.Uint64
	chown             atomic.Uint64
	chtimes           atomic.Uint64
	copies            atomic.Uint64
	bytes             atomic.Uint64
	totalBytesScanned atomic.Uint64
	deletedBytes      atomic.Uint64
	dirCount          atomic.Uint64
	// Reference to synchronizer for accessing in-progress operation counts
	synchronizer *csync.Synchronizer
}

// statsSnapshot captures a point-in-time view of collected statistics.
type statsSnapshot struct {
	lstat             uint64
	readdir           uint64
	mkdir             uint64
	unlink            uint64
	removeAll         uint64
	symlink           uint64
	chmod             uint64
	chown             uint64
	chtimes           uint64
	copies            uint64
	bytes             uint64
	totalBytesScanned uint64
	deletedBytes      uint64
	dirCount          uint64
	// Workers concurrency tracking
	workersLstat     uint64
	workersReaddir   uint64
	workersMkdir     uint64
	workersUnlink    uint64
	workersRemoveAll uint64
	workersSymlink   uint64
	workersChmod     uint64
	workersChown     uint64
	workersChtimes   uint64
	workersCopy      uint64
}

// snapshot returns a consistent view of current stats at a given moment.
func (s *statsCollector) snapshot() statsSnapshot {
	snap := statsSnapshot{
		lstat:             s.lstat.Load(),
		readdir:           s.readdir.Load(),
		mkdir:             s.mkdir.Load(),
		unlink:            s.unlink.Load(),
		removeAll:         s.removeAll.Load(),
		symlink:           s.symlink.Load(),
		chmod:             s.chmod.Load(),
		chown:             s.chown.Load(),
		chtimes:           s.chtimes.Load(),
		copies:            s.copies.Load(),
		bytes:             s.bytes.Load(),
		totalBytesScanned: s.totalBytesScanned.Load(),
		deletedBytes:      s.deletedBytes.Load(),
		dirCount:          s.dirCount.Load(),
	}
	// Get in-progress operation counts from synchronizer
	if s.synchronizer != nil {
		inProgress := s.synchronizer.GetInProgressOperations()
		snap.workersLstat = inProgress["lstat"]
		snap.workersReaddir = inProgress["readdir"]
		snap.workersMkdir = inProgress["mkdir"]
		snap.workersUnlink = inProgress["unlink"]
		snap.workersRemoveAll = inProgress["removeall"]
		snap.workersSymlink = inProgress["symlink"]
		snap.workersChmod = inProgress["chmod"]
		snap.workersChown = inProgress["chown"]
		snap.workersChtimes = inProgress["chtimes"]
		snap.workersCopy = inProgress["copy"]
	}
	return snap
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
	var statWorkers bool
	var logSlowOps bool

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
				return fmt.Errorf("workers must be >= 1")
			}

			if showWorkers {
				fmt.Printf("workers: cpus=%d workers=%d\n", runtime.NumCPU(), workers)
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
						if isDir {
							stats.dirCount.Add(1)
						} else if fileInfo != nil {
							stats.totalBytesScanned.Add(uint64(fileInfo.Size()))
						}
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
						relPath, _ := filepath.Rel(src, srcPath)
						logMsg("copy", fmt.Sprintf("%s (%s)", relPath, formatBytes(uint64(size))), err)
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
				OnUnlinkDetail: func(path string, size int64, err error) {
					if err == nil && size > 0 {
						stats.deletedBytes.Add(uint64(size))
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
				OnRemoveAllDetail: func(path string, size int64, err error) {
					if err == nil && size > 0 {
						stats.deletedBytes.Add(uint64(size))
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

			opts := csync.Options{IgnoreAtime: ignoreAtime, LogSlowOps: logSlowOps}
			syncer := csync.NewSynchronizerWithLoggerAndOptions(src, dst, workers, false, callbacks, nil, opts)
			stats.synchronizer = syncer

			var done chan struct{}
			if statsFlag || statWorkers {
				done = make(chan struct{})
			}
			if statsFlag {
				go runStatsPrinter(stats, done, start)
			}
			if statWorkers {
				go runWorkerStatsPrinter(syncer, done)
			}

			err := syncer.Run()
			if done != nil {
				close(done)
			}
			if statsFlag {
				// Print final stats
				finalStats := stats.snapshot()
				printFinalStats(finalStats, start)
			}
			return err
		},
	}

	rootCmd.Flags().BoolVarP(&verbose, "verbose", "v", false, "print verbose operation logs")
	rootCmd.Flags().BoolVar(&statsFlag, "stats", false, "print stats every 10s during sync")
	rootCmd.Flags().IntVar(&workers, "workers", 32, "maximum number of concurrent workers (>=1)")
	rootCmd.Flags().StringArrayVar(&excludes, "exclude", nil, "exclude entries whose base name matches (repeatable)")
	rootCmd.Flags().StringArrayVar(&logOpsFlag, "log-op", nil, "include additional operations in verbose output (comma-separated or repeatable)")
	rootCmd.Flags().StringArrayVar(&noLogOpsFlag, "no-log-op", nil, "exclude operations from verbose output (comma-separated or repeatable)")
	rootCmd.Flags().BoolVar(&ignoreAtime, "ignore-atime", false, "ignore atime differences when syncing times (preserve existing atime)")
	rootCmd.Flags().BoolVar(&showWorkers, "show-workers", false, "print detected CPUs and max worker count at start")
	rootCmd.Flags().BoolVar(&statWorkers, "stat-workers", false, "print worker state table every 10s during sync")
	rootCmd.Flags().BoolVar(&logSlowOps, "log-slow-ops", false, "log operations taking longer than 1s (repeats every second)")

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

// runWorkerStatsPrinter prints a worker state table every 10 seconds and on completion.
func runWorkerStatsPrinter(syncer *csync.Synchronizer, done <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			states := syncer.GetWorkerStates()
			printWorkerStatsTable(states)
		case <-done:
			states := syncer.GetWorkerStates()
			printWorkerStatsTable(states)
			return
		}
	}
}

// printWorkerStatsTable renders a table with all worker states.
func printWorkerStatsTable(states []csync.WorkerState) {
	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleRounded)
	t.Style().Color.Row = text.Colors{text.Reset}
	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 1, Align: text.AlignRight},
		{Number: 2, Align: text.AlignLeft},
		{Number: 3, Align: text.AlignLeft},
		{Number: 4, Align: text.AlignLeft},
		{Number: 5, Align: text.AlignLeft},
		{Number: 6, Align: text.AlignRight},
	})
	t.AppendHeader(table.Row{text.Bold.Sprint("Worker"), text.Bold.Sprint("State"), text.Bold.Sprint("Operation"), text.Bold.Sprint("Path"), text.Bold.Sprint("Duration"), text.Bold.Sprint("Queue")})

	for _, st := range states {
		duration := ""
		if st.Duration > 0 {
			duration = st.Duration.Truncate(time.Millisecond).String()
		}
		path := st.Path
		t.AppendRow(table.Row{
			st.ID,
			st.State,
			st.Operation,
			path,
			duration,
			st.QueueLen,
		})
	}

	t.Render()
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
	var percents []string
	var totals []string
	var avgs []string
	var intervals []string

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.SetStyle(table.StyleRounded)
	t.Style().Color.Row = text.Colors{text.Reset}
	t.SetColumnConfigs([]table.ColumnConfig{
		{Number: 2, Align: text.AlignRight},
		{Number: 3, Align: text.AlignLeft},
		{Number: 4, Align: text.AlignLeft},
		{Number: 5, Align: text.AlignLeft},
		{Number: 6, Align: text.AlignRight},
	})
	t.AppendHeader(table.Row{text.Bold.Sprint("Operation"), text.Bold.Sprint("%"), text.Bold.Sprint("Total"), text.Bold.Sprint("Avg/s"), text.Bold.Sprint("Avg/s (interval)"), text.Bold.Sprint("Workers")})

	lstatTotal := cur.lstat
	for _, r := range rows {
		if r.total == 0 {
			continue
		}
		prevTotal := getPrevTotal(prev, r.name)
		totalRate := float64(r.total) / elapsed
		intervalRate := float64(r.total-prevTotal) / interval
		names = append(names, r.name)
		pct := ""
		if r.name != "lstat" && lstatTotal > 0 {
			percentage := (float64(r.total) * 100) / float64(lstatTotal)
			pct = formatPercent(percentage)
		}
		percents = append(percents, pct)
		totals = append(totals, formatScaledUint(r.total, ""))
		avgs = append(avgs, formatAvgRate(totalRate, "/s"))

		// Format interval rate with conditional styling
		intervalStr := formatScaledFloat(intervalRate, "/s")
		if totalRate > 0 {
			ratio := intervalRate / totalRate
			if ratio > 1.5 {
				intervalStr = text.Bold.Sprint(intervalStr)
			} else if ratio < 2.0/3.0 {
				intervalStr = text.FgHiBlack.Sprint(intervalStr)
			}
		}
		intervals = append(intervals, intervalStr)
	}

	prevBytes := prev.bytes
	bytesRateTotal := float64(cur.bytes) / elapsed
	bytesRateInterval := float64(cur.bytes-prevBytes) / interval
	if cur.bytes > 0 {
		names = append(names, "bytes")
		bytesPct := ""
		if cur.totalBytesScanned > 0 {
			percentage := (float64(cur.bytes) * 100) / float64(cur.totalBytesScanned)
			bytesPct = formatPercent(percentage)
		}
		percents = append(percents, bytesPct)
		totals = append(totals, formatScaledBytesUint(cur.bytes))
		avgs = append(avgs, formatAvgBytesRate(bytesRateTotal, "/s"))

		// Format bytes interval rate with conditional styling
		bytesIntervalStr := formatScaledBytesFloat(bytesRateInterval, "/s")
		if bytesRateTotal > 0 {
			ratio := bytesRateInterval / bytesRateTotal
			if ratio > 1.5 {
				bytesIntervalStr = text.Bold.Sprint(bytesIntervalStr)
			} else if ratio < 2.0/3.0 {
				bytesIntervalStr = text.FgHiBlack.Sprint(bytesIntervalStr)
			}
		}
		intervals = append(intervals, bytesIntervalStr)
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
		workersStr := ""
		switch name {
		case "lstat":
			if cur.workersLstat > 0 {
				workersStr = formatScaledUint(cur.workersLstat, "")
			}
		case "readdir":
			if cur.workersReaddir > 0 {
				workersStr = formatScaledUint(cur.workersReaddir, "")
			}
		case "mkdir":
			if cur.workersMkdir > 0 {
				workersStr = formatScaledUint(cur.workersMkdir, "")
			}
		case "unlink":
			if cur.workersUnlink > 0 {
				workersStr = formatScaledUint(cur.workersUnlink, "")
			}
		case "removeall":
			if cur.workersRemoveAll > 0 {
				workersStr = formatScaledUint(cur.workersRemoveAll, "")
			}
		case "symlink":
			if cur.workersSymlink > 0 {
				workersStr = formatScaledUint(cur.workersSymlink, "")
			}
		case "chmod":
			if cur.workersChmod > 0 {
				workersStr = formatScaledUint(cur.workersChmod, "")
			}
		case "chown":
			if cur.workersChown > 0 {
				workersStr = formatScaledUint(cur.workersChown, "")
			}
		case "chtimes":
			if cur.workersChtimes > 0 {
				workersStr = formatScaledUint(cur.workersChtimes, "")
			}
		case "copy":
			if cur.workersCopy > 0 {
				workersStr = formatScaledUint(cur.workersCopy, "")
			}
		}
		t.AppendRow(table.Row{
			text.Bold.Sprint(name),
			percents[i],
			totals[i],
			avgs[i],
			intervals[i],
			workersStr,
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
	if n == 0 {
		return ""
	}
	if n < 1000 {
		return fmt.Sprintf("%d%s", n, suffix)
	}
	return formatScaledFloat(float64(n), suffix)
}

// formatScaledBytesUint renders a byte count using uppercase scaled units without a "B" suffix.
// Returns an empty string when the value is zero.
func formatScaledBytesUint(n uint64) string {
	if n == 0 {
		return ""
	}
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	return formatScaledBytesFloat(float64(n), "")
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

// formatScaledBytesFloat renders a byte rate/size using uppercase units (K, M, G...) without a "B" suffix.
// Returns an empty string when the value is zero.
func formatScaledBytesFloat(v float64, suffix string) string {
	if v == 0 {
		return ""
	}
	units := []string{"", "K", "M", "G", "T", "P", "E"}
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

// formatPercent renders a percentage value (without %) applying "<" for tiny values,
// omitting leading zero for values < 1, and dimming the fractional part when
// the integer part has two or more digits.
func formatPercent(value float64) string {
	if value <= 0 {
		return ""
	}
	if value < 0.05 {
		return text.FgHiBlack.Sprint("<")
	}
	formatted := fmt.Sprintf("%.1f", value)
	return stylizeFraction(omitLeadingZero(formatted))
}

// formatAvgRate renders a scaled rate (without suffix) and the same styling rules as percentages.
func formatAvgRate(value float64, suffix string) string {
	if value <= 0 {
		return ""
	}
	if value < 0.05 {
		return text.FgHiBlack.Sprint("<")
	}
	formatted := formatScaledFloat(value, "")
	return stylizeFraction(omitLeadingZero(formatted))
}

// formatAvgBytesRate renders a scaled byte rate (without suffix) and the same styling rules.
func formatAvgBytesRate(value float64, suffix string) string {
	if value <= 0 {
		return ""
	}
	if value < 0.05 {
		return text.FgHiBlack.Sprint("<")
	}
	formatted := formatScaledBytesFloat(value, "")
	return stylizeFraction(omitLeadingZero(formatted))
}

// omitLeadingZero removes a leading "0" for values like "0.5" or "0.5/s".
func omitLeadingZero(s string) string {
	if strings.HasPrefix(s, "0.") {
		return s[1:]
	}
	return s
}

// stylizeFraction dims the decimal point and fractional digits when the integer part
// has two or more digits, but preserves the scale suffix (k, m, K, M, etc.) in white.
// It keeps alignment intact by only coloring the relevant segment.
func stylizeFraction(s string) string {
	plain := stripANSI(s)
	dot := strings.IndexByte(plain, '.')
	if dot == -1 {
		return s
	}
	intPart := plain[:dot]
	if len(intPart) < 2 {
		return s
	}

	// Find where fractional digits end and scale suffix begins.
	// Fractional digits are immediately after the dot; scale suffix follows.
	fracEnd := dot + 1
	for fracEnd < len(plain) && plain[fracEnd] >= '0' && plain[fracEnd] <= '9' {
		fracEnd++
	}

	// fracEnd is now at the first non-digit character after the decimal point (the scale).
	// Find the corresponding indices in the styled string.
	dotIndex := indexInStyled(s, dot)
	fracEndIndex := indexInStyled(s, fracEnd)

	if dotIndex == -1 || fracEndIndex == -1 || dotIndex >= len(s) || fracEndIndex > len(s) {
		return s
	}

	// Reconstruct: unchanged part + dimmed (dot + fractional digits) + scale suffix
	return s[:dotIndex] + text.FgHiBlack.Sprint(s[dotIndex:fracEndIndex]) + s[fracEndIndex:]
}

// indexInStyled maps an index in the plain string to the styled string index.
func indexInStyled(styled string, plainIndex int) int {
	pi := 0
	for i := 0; i < len(styled); i++ {
		if styled[i] == '\x1b' && i+1 < len(styled) && styled[i+1] == '[' {
			i += 2
			for i < len(styled) {
				c := styled[i]
				if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
					break
				}
				i++
			}
			continue
		}
		if pi == plainIndex {
			return i
		}
		pi++
	}
	return -1
}

// alignDecimal pads values so their decimal points line up in a column.
func alignDecimal(values []string) []string {
	maxInt := 0
	for _, v := range values {
		if v == "" {
			continue
		}
		plain := stripANSI(v)
		dot := strings.IndexByte(plain, '.')
		if dot == -1 {
			dot = len(plain)
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
		plain := stripANSI(v)
		dot := strings.IndexByte(plain, '.')
		if dot == -1 {
			dot = len(plain)
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

// stripANSI removes ANSI color sequences for alignment calculations.
func stripANSI(s string) string {
	if !strings.Contains(s, "\x1b[") {
		return s
	}
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		if s[i] == '\x1b' && i+1 < len(s) && s[i+1] == '[' {
			i += 2
			for i < len(s) {
				c := s[i]
				if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') {
					break
				}
				i++
			}
			continue
		}
		b.WriteByte(s[i])
	}
	return b.String()
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

// printFinalStats displays comprehensive final statistics after sync completion.
func printFinalStats(stats statsSnapshot, start time.Time) {
	elapsed := time.Since(start)
	// Remove sub-second precision from duration
	elapsedTrunc := elapsed.Truncate(time.Second)

	fmt.Println()
	fmt.Println("Final Statistics:")
	fmt.Printf("  Total Duration:     %v\n", elapsedTrunc)
	inodeCount := formatScaledUint(stats.lstat, "")
	if inodeCount == "" {
		inodeCount = "0"
	}
	dirCount := formatScaledUint(stats.dirCount, "")
	if dirCount == "" {
		dirCount = "0"
	}
	totalSize := formatScaledBytesUint(stats.totalBytesScanned)
	if totalSize == "" {
		totalSize = "0"
	}
	copySize := formatScaledBytesUint(stats.bytes)
	if copySize == "" {
		copySize = "0"
	}
	deletedSize := formatScaledBytesUint(stats.deletedBytes)
	if deletedSize == "" {
		deletedSize = "0"
	}
	netSize := int64(stats.bytes) - int64(stats.deletedBytes)
	var netStr string
	if netSize >= 0 {
		netStr = formatScaledBytesUint(uint64(netSize))
	} else {
		netStr = fmt.Sprintf("-%s", formatScaledBytesUint(uint64(-netSize)))
	}
	if netStr == "" {
		netStr = "0"
	}

	fmt.Printf("  Inode count:        %s\n", inodeCount)
	fmt.Printf("  Directory count:    %s\n", dirCount)
	fmt.Printf("  Total data size:    %s\n", totalSize)
	fmt.Printf("  Copied data size:   %s\n", copySize)
	fmt.Printf("  Deleted data size:  %s\n", deletedSize)
	fmt.Printf("  Net total:          %s\n", netStr)

	if elapsed.Seconds() > 0 {
		copySpeed := float64(stats.bytes) / elapsed.Seconds()
		lstatSpeed := float64(stats.lstat) / elapsed.Seconds()
		fmt.Printf("  Copy speed:         %s\n", formatScaledBytesFloat(copySpeed, "/s"))
		fmt.Printf("  Inode scan speed:   %s\n", formatScaledFloat(lstatSpeed, "/s"))
	}
}
