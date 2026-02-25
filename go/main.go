package main

import (
	"bufio"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/writer"
)

type Config struct {
	Mode                string
	DataDir             string
	InputGlobs          []string
	DownloadDate        string
	DownloadHours       int
	ParquetCompression  string
}

var config = Config{
	Mode:               "cpu", // "cpu" (strict validation) or "io" (relaxed validation)
	DataDir:            "data",
	InputGlobs:         []string{},
	DownloadDate:       "2025-01-01", // set to "" to disable downloads
	DownloadHours:      1,
	ParquetCompression: "ZSTD",
}

type Stats struct {
	TotalLines   int64
	ValidLines   int64
	InvalidLines int64
}

type AggKey struct {
	EventDay  string
	EventType string
}

type AggValue struct {
	Count     int64
	SumRepoID int64
}

type ParquetRow struct {
	EventDay    string `parquet:"name=event_day, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	EventType   string `parquet:"name=event_type, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	RecordCount int64  `parquet:"name=record_count, type=INT64"`
	SumRepoID   int64  `parquet:"name=sum_repo_id, type=INT64"`
}

func ensureParent(path string) error {
	return os.MkdirAll(filepath.Dir(path), 0o755)
}

func downloadGHArchive(dateStr string, hours int, downloadDir string) ([]string, error) {
	if dateStr == "" || hours <= 0 {
		return []string{}, nil
	}
	dateObj, err := time.Parse("2006-01-02", dateStr)
	if err != nil {
		return nil, fmt.Errorf("invalid download date: %w", err)
	}
	if err := os.MkdirAll(downloadDir, 0o755); err != nil {
		return nil, err
	}

	client := &http.Client{Timeout: 5 * time.Minute}
	downloaded := make([]string, 0, hours)

	for hour := 0; hour < hours; hour++ {
		ts := dateObj.Add(time.Duration(hour) * time.Hour)
		url := fmt.Sprintf("https://data.gharchive.org/%s-%d.json.gz", ts.Format("2006-01-02"), ts.Hour())
		outPath := filepath.Join(downloadDir, fmt.Sprintf("%s-%d.json.gz", ts.Format("2006-01-02"), ts.Hour()))

		if _, err := os.Stat(outPath); err == nil {
			fmt.Printf("[download] skip existing %s\n", outPath)
			downloaded = append(downloaded, outPath)
			continue
		}

		fmt.Printf("[download] %s -> %s\n", url, outPath)
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("User-Agent", "Mozilla/5.0")

		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("download failed with status %s", resp.Status)
		}

		outFile, err := os.Create(outPath)
		if err != nil {
			resp.Body.Close()
			return nil, err
		}
		_, copyErr := io.Copy(outFile, resp.Body)
		closeErr := outFile.Close()
		resp.Body.Close()
		if copyErr != nil {
			return nil, copyErr
		}
		if closeErr != nil {
			return nil, closeErr
		}
		downloaded = append(downloaded, outPath)
	}
	return downloaded, nil
}

func resolveInputFiles(patterns []string) ([]string, error) {
	seen := map[string]bool{}
	files := make([]string, 0)
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return nil, err
		}
		sort.Strings(matches)
		for _, m := range matches {
			if !seen[m] {
				seen[m] = true
				files = append(files, m)
			}
		}
	}
	sort.Strings(files)
	return files, nil
}

func getStringField(record map[string]any, key string) (string, bool) {
	v, ok := record[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	if !ok || s == "" {
		return "", false
	}
	return s, true
}

func getRepoID(record map[string]any) (int64, bool) {
	repoRaw, ok := record["repo"]
	if !ok {
		return 0, false
	}
	repo, ok := repoRaw.(map[string]any)
	if !ok {
		return 0, false
	}
	idRaw, ok := repo["id"]
	if !ok {
		return 0, false
	}
	switch v := idRaw.(type) {
	case float64:
		return int64(v), true
	case int64:
		return v, true
	case string:
		n, err := strconv.ParseInt(v, 10, 64)
		return n, err == nil
	default:
		return 0, false
	}
}

func validateRecord(record map[string]any, strict bool) bool {
	recordID, ok := getStringField(record, "id")
	if !ok || recordID == "" {
		return false
	}
	eventType, ok := getStringField(record, "type")
	if !ok || eventType == "" {
		return false
	}
	createdAt, ok := getStringField(record, "created_at")
	if !ok || createdAt == "" {
		return false
	}
	_, ok = getRepoID(record)
	if !ok {
		return false
	}

	if strict {
		if !strings.Contains(createdAt, "T") || !strings.HasSuffix(createdAt, "Z") {
			return false
		}
		if len(eventType) > 100 {
			return false
		}
	}
	return true
}

func transformRecord(record map[string]any) (string, string, int64, bool) {
	createdAt, ok := getStringField(record, "created_at")
	if !ok || len(createdAt) < 10 {
		return "", "", 0, false
	}
	eventType, ok := getStringField(record, "type")
	if !ok {
		return "", "", 0, false
	}
	repoID, ok := getRepoID(record)
	if !ok {
		return "", "", 0, false
	}
	eventDay := createdAt[:10]
	return eventDay, strings.ToLower(strings.TrimSpace(eventType)), repoID, true
}

func processFiles(inputFiles []string, strict bool) (map[AggKey]AggValue, Stats, error) {
	aggregates := map[AggKey]AggValue{}
	stats := Stats{}

	for _, filePath := range inputFiles {
		fmt.Printf("[process] %s\n", filePath)
		f, err := os.Open(filePath)
		if err != nil {
			return nil, stats, err
		}
		gz, err := gzip.NewReader(f)
		if err != nil {
			f.Close()
			return nil, stats, err
		}

		scanner := bufio.NewScanner(gz)
		// Increase scanner buffer for large JSON lines.
		buf := make([]byte, 0, 1024*1024)
		scanner.Buffer(buf, 16*1024*1024)

		for scanner.Scan() {
			stats.TotalLines++
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				stats.InvalidLines++
				continue
			}

			record := map[string]any{}
			if err := json.Unmarshal([]byte(line), &record); err != nil {
				stats.InvalidLines++
				continue
			}

			if !validateRecord(record, strict) {
				stats.InvalidLines++
				continue
			}

			eventDay, eventType, repoID, ok := transformRecord(record)
			if !ok {
				stats.InvalidLines++
				continue
			}

			stats.ValidLines++
			key := AggKey{EventDay: eventDay, EventType: eventType}
			v := aggregates[key]
			v.Count++
			v.SumRepoID += repoID
			aggregates[key] = v
		}

		if err := scanner.Err(); err != nil {
			gz.Close()
			f.Close()
			return nil, stats, err
		}
		gz.Close()
		f.Close()
	}
	return aggregates, stats, nil
}

func sortedRows(aggregates map[AggKey]AggValue) []ParquetRow {
	rows := make([]ParquetRow, 0, len(aggregates))
	for k, v := range aggregates {
		rows = append(rows, ParquetRow{
			EventDay:    k.EventDay,
			EventType:   k.EventType,
			RecordCount: v.Count,
			SumRepoID:   v.SumRepoID,
		})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].EventDay == rows[j].EventDay {
			return rows[i].EventType < rows[j].EventType
		}
		return rows[i].EventDay < rows[j].EventDay
	})
	return rows
}

func writeParquet(path string, rows []ParquetRow) error {
	if err := ensureParent(path); err != nil {
		return err
	}
	fw, err := local.NewLocalFileWriter(path)
	if err != nil {
		return err
	}
	pw, err := writer.NewParquetWriter(fw, new(ParquetRow), 1)
	if err != nil {
		fw.Close()
		return err
	}
	pw.CompressionType = 6 // ZSTD
	for _, row := range rows {
		if err := pw.Write(row); err != nil {
			pw.WriteStop()
			fw.Close()
			return err
		}
	}
	if err := pw.WriteStop(); err != nil {
		fw.Close()
		return err
	}
	return fw.Close()
}

func writeChecksum(path string, rows []ParquetRow) (string, error) {
	if err := ensureParent(path); err != nil {
		return "", err
	}
	h := sha256.New()
	for _, r := range rows {
		line := fmt.Sprintf("%s|%s|%d|%d\n", r.EventDay, r.EventType, r.RecordCount, r.SumRepoID)
		if _, err := h.Write([]byte(line)); err != nil {
			return "", err
		}
	}
	checksum := hex.EncodeToString(h.Sum(nil))
	if err := os.WriteFile(path, []byte(checksum+"\n"), 0o644); err != nil {
		return "", err
	}
	return checksum, nil
}

func dedupeAndSort(files []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(files))
	for _, f := range files {
		if !seen[f] {
			seen[f] = true
			out = append(out, f)
		}
	}
	sort.Strings(out)
	return out
}

func resolveDataDir() string {
	candidates := []string{
		config.DataDir,
		"data",
		"../data",
	}
	seen := map[string]bool{}
	for _, c := range candidates {
		if c == "" || seen[c] {
			continue
		}
		seen[c] = true
		info, err := os.Stat(c)
		if err == nil && info.IsDir() {
			return c
		}
	}
	return config.DataDir
}

func main() {
	mode := strings.ToLower(config.Mode)
	if mode != "cpu" && mode != "io" {
		fmt.Fprintln(os.Stderr, "CONFIG mode must be cpu or io")
		os.Exit(1)
	}
	strict := mode == "cpu"

	dataDir := resolveDataDir()
	downloadDir := filepath.Join(dataDir, "raw", "gharchive")
	outputParquet := filepath.Join(dataDir, "out", "go_cpu_1h.parquet")
	outputChecksum := filepath.Join(dataDir, "out", "go_cpu_1h.sha256")

	downloaded, err := downloadGHArchive(config.DownloadDate, config.DownloadHours, downloadDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "download error: %v\n", err)
		os.Exit(1)
	}

	resolved, err := resolveInputFiles(config.InputGlobs)
	if err != nil {
		fmt.Fprintf(os.Stderr, "glob error: %v\n", err)
		os.Exit(1)
	}
	inputFiles := dedupeAndSort(append(resolved, downloaded...))
	if len(inputFiles) == 0 {
		fmt.Fprintln(os.Stderr, "No input files found. Set config.InputGlobs and/or download settings.")
		os.Exit(1)
	}

	aggregates, stats, err := processFiles(inputFiles, strict)
	if err != nil {
		fmt.Fprintf(os.Stderr, "processing error: %v\n", err)
		os.Exit(1)
	}

	rows := sortedRows(aggregates)
	if err := writeParquet(outputParquet, rows); err != nil {
		fmt.Fprintf(os.Stderr, "parquet error: %v\n", err)
		os.Exit(1)
	}
	checksum, err := writeChecksum(outputChecksum, rows)
	if err != nil {
		fmt.Fprintf(os.Stderr, "checksum error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("[done]")
	fmt.Printf("mode=%s\n", mode)
	fmt.Printf("input_files=%d\n", len(inputFiles))
	fmt.Printf("total_lines=%d\n", stats.TotalLines)
	fmt.Printf("valid_lines=%d\n", stats.ValidLines)
	fmt.Printf("invalid_lines=%d\n", stats.InvalidLines)
	fmt.Printf("aggregate_rows=%d\n", len(rows))
	fmt.Printf("checksum=%s\n", checksum)
	fmt.Printf("parquet=%s\n", outputParquet)
	fmt.Printf("checksum_file=%s\n", outputChecksum)
}
