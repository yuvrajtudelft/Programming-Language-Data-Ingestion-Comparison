use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use chrono::{Duration, NaiveDate};
use flate2::read::GzDecoder;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use reqwest::blocking::Client;
use serde_json::Value;
use sha2::Digest;

struct Config {
    mode: &'static str,
    data_dir: &'static str,
    input_globs: &'static [&'static str],
    download_date: &'static str, // set "" to disable downloads
    download_hours: usize,
    output_parquet: &'static str,
    output_checksum: &'static str,
}

const CONFIG: Config = Config {
    mode: "cpu",
    data_dir: "data",
    input_globs: &[],
    download_date: "2025-01-01",
    download_hours: 1,
    output_parquet: "out/rust_cpu_1h.parquet",
    output_checksum: "out/rust_cpu_1h.sha256",
};

#[derive(Default)]
struct Stats {
    total_lines: u64,
    valid_lines: u64,
    invalid_lines: u64,
}

#[derive(Clone, Eq, PartialEq, Hash)]
struct AggKey {
    event_day: String,
    event_type: String,
}

#[derive(Default, Clone, Copy)]
struct AggValue {
    count: i64,
    sum_repo_id: i64,
}

fn ensure_parent(path: &Path) -> Result<(), String> {
    let parent = path
        .parent()
        .ok_or_else(|| format!("Path has no parent: {}", path.display()))?;
    fs::create_dir_all(parent).map_err(|e| format!("create_dir_all failed: {e}"))
}

fn resolve_data_dir() -> PathBuf {
    let candidates = [PathBuf::from(CONFIG.data_dir), PathBuf::from("data"), PathBuf::from("../data")];
    for c in candidates {
        if c.is_dir() {
            return c;
        }
    }
    PathBuf::from(CONFIG.data_dir)
}

fn download_gharchive(date_str: &str, hours: usize, download_dir: &Path) -> Result<Vec<PathBuf>, String> {
    if date_str.is_empty() || hours == 0 {
        return Ok(Vec::new());
    }
    let date = NaiveDate::parse_from_str(date_str, "%Y-%m-%d")
        .map_err(|e| format!("Invalid date {date_str}: {e}"))?;
    fs::create_dir_all(download_dir).map_err(|e| format!("mkdir failed: {e}"))?;

    let client = Client::builder()
        .user_agent("Mozilla/5.0")
        .build()
        .map_err(|e| format!("reqwest client build failed: {e}"))?;

    let mut downloaded = Vec::new();
    for hour in 0..hours {
        let ts = date
            .and_hms_opt(0, 0, 0)
            .ok_or_else(|| "Invalid base timestamp".to_string())?
            + Duration::hours(hour as i64);
        let date_part = ts.format("%Y-%m-%d").to_string();
        let hour_part = ts.format("%-H").to_string();
        let url = format!("https://data.gharchive.org/{date_part}-{hour_part}.json.gz");
        let out_path = download_dir.join(format!("{date_part}-{hour_part}.json.gz"));

        if out_path.exists() {
            println!("[download] skip existing {}", out_path.display());
            downloaded.push(out_path);
            continue;
        }

        println!("[download] {} -> {}", url, out_path.display());
        let mut resp = client
            .get(url)
            .send()
            .map_err(|e| format!("download failed: {e}"))?;
        if !resp.status().is_success() {
            return Err(format!("download status: {}", resp.status()));
        }
        let mut out = File::create(&out_path).map_err(|e| format!("file create failed: {e}"))?;
        resp.copy_to(&mut out)
            .map_err(|e| format!("write download failed: {e}"))?;
        downloaded.push(out_path);
    }
    Ok(downloaded)
}

fn resolve_input_files(patterns: &[&str]) -> Result<Vec<PathBuf>, String> {
    let mut files = Vec::new();
    let mut seen: HashSet<PathBuf> = HashSet::new();
    for pattern in patterns {
        for entry in glob::glob(pattern).map_err(|e| format!("invalid glob {pattern}: {e}"))? {
            let p = entry.map_err(|e| format!("glob entry error: {e}"))?;
            if seen.insert(p.clone()) {
                files.push(p);
            }
        }
    }
    files.sort();
    Ok(files)
}

fn get_repo_id(record: &Value) -> Option<i64> {
    let repo = record.get("repo")?;
    let id = repo.get("id")?;
    if let Some(v) = id.as_i64() {
        return Some(v);
    }
    if let Some(v) = id.as_u64() {
        return i64::try_from(v).ok();
    }
    if let Some(v) = id.as_str() {
        return v.parse::<i64>().ok();
    }
    None
}

fn validate_record(record: &Value, strict: bool) -> bool {
    let record_id = record.get("id").and_then(Value::as_str).unwrap_or_default();
    let event_type = record.get("type").and_then(Value::as_str).unwrap_or_default();
    let created_at = record.get("created_at").and_then(Value::as_str).unwrap_or_default();
    if record_id.is_empty() || event_type.is_empty() || created_at.is_empty() {
        return false;
    }
    if get_repo_id(record).is_none() {
        return false;
    }
    if strict {
        if !created_at.contains('T') || !created_at.ends_with('Z') {
            return false;
        }
        if event_type.len() > 100 {
            return false;
        }
    }
    true
}

fn transform_record(record: &Value) -> Option<(String, String, i64)> {
    let created_at = record.get("created_at")?.as_str()?;
    if created_at.len() < 10 {
        return None;
    }
    let event_day = created_at[..10].to_string();
    let event_type = record.get("type")?.as_str()?.trim().to_lowercase();
    let repo_id = get_repo_id(record)?;
    Some((event_day, event_type, repo_id))
}

fn process_files(input_files: &[PathBuf], strict: bool) -> Result<(HashMap<AggKey, AggValue>, Stats), String> {
    let mut aggregates: HashMap<AggKey, AggValue> = HashMap::new();
    let mut stats = Stats::default();

    for path in input_files {
        println!("[process] {}", path.display());
        let f = File::open(path).map_err(|e| format!("open {} failed: {e}", path.display()))?;
        let gz = GzDecoder::new(f);
        let reader = BufReader::new(gz);
        for line_res in reader.lines() {
            stats.total_lines += 1;
            let line = match line_res {
                Ok(v) => v,
                Err(_) => {
                    stats.invalid_lines += 1;
                    continue;
                }
            };
            let line = line.trim();
            if line.is_empty() {
                stats.invalid_lines += 1;
                continue;
            }
            let record: Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(_) => {
                    stats.invalid_lines += 1;
                    continue;
                }
            };
            if !validate_record(&record, strict) {
                stats.invalid_lines += 1;
                continue;
            }
            let (event_day, event_type, repo_id) = match transform_record(&record) {
                Some(v) => v,
                None => {
                    stats.invalid_lines += 1;
                    continue;
                }
            };
            stats.valid_lines += 1;
            let key = AggKey { event_day, event_type };
            let entry = aggregates.entry(key).or_default();
            entry.count += 1;
            entry.sum_repo_id += repo_id;
        }
    }
    Ok((aggregates, stats))
}

#[derive(Clone)]
struct Row {
    event_day: String,
    event_type: String,
    record_count: i64,
    sum_repo_id: i64,
}

fn sorted_rows(aggregates: &HashMap<AggKey, AggValue>) -> Vec<Row> {
    let mut rows: Vec<Row> = aggregates
        .iter()
        .map(|(k, v)| Row {
            event_day: k.event_day.clone(),
            event_type: k.event_type.clone(),
            record_count: v.count,
            sum_repo_id: v.sum_repo_id,
        })
        .collect();
    rows.sort_by(|a, b| {
        if a.event_day == b.event_day {
            a.event_type.cmp(&b.event_type)
        } else {
            a.event_day.cmp(&b.event_day)
        }
    });
    rows
}

fn write_parquet(path: &Path, rows: &[Row]) -> Result<(), String> {
    ensure_parent(path)?;
    let schema = Arc::new(Schema::new(vec![
        Field::new("event_day", DataType::Utf8, false),
        Field::new("event_type", DataType::Utf8, false),
        Field::new("record_count", DataType::Int64, false),
        Field::new("sum_repo_id", DataType::Int64, false),
    ]));

    let event_day: Vec<&str> = rows.iter().map(|r| r.event_day.as_str()).collect();
    let event_type: Vec<&str> = rows.iter().map(|r| r.event_type.as_str()).collect();
    let record_count: Vec<i64> = rows.iter().map(|r| r.record_count).collect();
    let sum_repo_id: Vec<i64> = rows.iter().map(|r| r.sum_repo_id).collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(event_day)) as ArrayRef,
            Arc::new(StringArray::from(event_type)) as ArrayRef,
            Arc::new(Int64Array::from(record_count)) as ArrayRef,
            Arc::new(Int64Array::from(sum_repo_id)) as ArrayRef,
        ],
    )
    .map_err(|e| format!("record batch build failed: {e}"))?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();

    let out = File::create(path).map_err(|e| format!("parquet create failed: {e}"))?;
    let mut writer =
        ArrowWriter::try_new(out, schema, Some(props)).map_err(|e| format!("writer init failed: {e}"))?;
    writer.write(&batch).map_err(|e| format!("parquet write failed: {e}"))?;
    writer.close().map_err(|e| format!("parquet close failed: {e}"))?;
    Ok(())
}

fn write_checksum(path: &Path, rows: &[Row]) -> Result<String, String> {
    ensure_parent(path)?;
    let mut ctx = sha2::Sha256::new();
    for r in rows {
        let line = format!("{}|{}|{}|{}\n", r.event_day, r.event_type, r.record_count, r.sum_repo_id);
        ctx.update(line.as_bytes());
    }
    let checksum = format!("{:x}", ctx.finalize());
    let mut out = File::create(path).map_err(|e| format!("checksum file create failed: {e}"))?;
    out.write_all(format!("{checksum}\n").as_bytes())
        .map_err(|e| format!("checksum write failed: {e}"))?;
    Ok(checksum)
}

fn main() {
    let mode = CONFIG.mode.to_lowercase();
    if mode != "cpu" && mode != "io" {
        eprintln!("CONFIG mode must be cpu or io");
        std::process::exit(1);
    }
    let strict = mode == "cpu";

    let data_dir = resolve_data_dir();
    let download_dir = data_dir.join("raw").join("gharchive");
    let output_parquet = data_dir.join(CONFIG.output_parquet);
    let output_checksum = data_dir.join(CONFIG.output_checksum);

    let downloaded = match download_gharchive(CONFIG.download_date, CONFIG.download_hours, &download_dir) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("download error: {e}");
            std::process::exit(1);
        }
    };

    let mut resolved = match resolve_input_files(CONFIG.input_globs) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("glob error: {e}");
            std::process::exit(1);
        }
    };
    resolved.extend(downloaded);
    resolved.sort();
    resolved.dedup();
    if resolved.is_empty() {
        eprintln!("No input files found. Set input_globs and/or download settings.");
        std::process::exit(1);
    }

    let (aggregates, stats) = match process_files(&resolved, strict) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("processing error: {e}");
            std::process::exit(1);
        }
    };
    let rows = sorted_rows(&aggregates);

    if let Err(e) = write_parquet(&output_parquet, &rows) {
        eprintln!("parquet error: {e}");
        std::process::exit(1);
    }
    let checksum = match write_checksum(&output_checksum, &rows) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("checksum error: {e}");
            std::process::exit(1);
        }
    };

    println!("[done]");
    println!("mode={mode}");
    println!("input_files={}", resolved.len());
    println!("total_lines={}", stats.total_lines);
    println!("valid_lines={}", stats.valid_lines);
    println!("invalid_lines={}", stats.invalid_lines);
    println!("aggregate_rows={}", rows.len());
    println!("checksum={checksum}");
    println!("parquet={}", output_parquet.display());
    println!("checksum_file={}", output_checksum.display());
}
