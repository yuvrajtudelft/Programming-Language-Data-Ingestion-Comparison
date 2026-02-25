package org.tud;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.MessageDigest;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

public class IngestBenchmark {
    private static final String MODE = "cpu"; // "cpu" (strict) or "io" (relaxed)
    private static final String DATA_DIR = "data";
    private static final String DOWNLOAD_DATE = "2025-01-01"; // set "" to disable downloads
    private static final int DOWNLOAD_HOURS = 1;
    private static final String OUTPUT_PARQUET_REL = "out/java_cpu_1h.parquet";
    private static final String OUTPUT_CHECKSUM_REL = "out/java_cpu_1h.sha256";
    private static final String[] INPUT_GLOBS = {};

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private static class Stats {
        long totalLines;
        long validLines;
        long invalidLines;
    }

    private static class AggValue {
        long count;
        long sumRepoId;
    }

    private record Row(String eventDay, String eventType, long recordCount, long sumRepoId) {}

    public static void main(String[] args) throws Exception {
        String mode = MODE.toLowerCase(Locale.ROOT);
        if (!mode.equals("cpu") && !mode.equals("io")) {
            throw new IllegalArgumentException("MODE must be cpu or io");
        }
        boolean strict = mode.equals("cpu");

        String dataDir = resolveDataDir();
        String downloadDir = Paths.get(dataDir, "raw", "gharchive").toString();
        String outputParquet = Paths.get(dataDir, OUTPUT_PARQUET_REL).toString();
        String outputChecksum = Paths.get(dataDir, OUTPUT_CHECKSUM_REL).toString();

        List<String> downloaded = downloadGhArchive(DOWNLOAD_DATE, DOWNLOAD_HOURS, downloadDir);
        List<String> inputFiles = new ArrayList<>(resolveInputFiles(INPUT_GLOBS));
        inputFiles.addAll(downloaded);
        inputFiles = dedupeAndSort(inputFiles);
        if (inputFiles.isEmpty()) {
            throw new IllegalStateException("No input files found. Set INPUT_GLOBS and/or download settings.");
        }

        Stats stats = new Stats();
        Map<String, AggValue> aggregates = processFiles(inputFiles, strict, stats);
        List<Row> rows = sortedRows(aggregates);
        writeParquet(outputParquet, rows);
        String checksum = writeChecksum(outputChecksum, rows);

        System.out.println("[done]");
        System.out.println("mode=" + mode);
        System.out.println("input_files=" + inputFiles.size());
        System.out.println("total_lines=" + stats.totalLines);
        System.out.println("valid_lines=" + stats.validLines);
        System.out.println("invalid_lines=" + stats.invalidLines);
        System.out.println("aggregate_rows=" + rows.size());
        System.out.println("checksum=" + checksum);
        System.out.println("parquet=" + outputParquet);
        System.out.println("checksum_file=" + outputChecksum);
    }

    private static String resolveDataDir() {
        String[] candidates = {DATA_DIR, "data", "../data"};
        for (String c : candidates) {
            File f = new File(c);
            if (f.exists() && f.isDirectory()) {
                return c;
            }
        }
        return DATA_DIR;
    }

    private static void ensureParent(String filePath) throws Exception {
        File parent = new File(filePath).getParentFile();
        if (parent != null && !parent.exists()) {
            if (!parent.mkdirs()) {
                throw new RuntimeException("Failed to create dir: " + parent.getAbsolutePath());
            }
        }
    }

    private static List<String> downloadGhArchive(String dateStr, int hours, String downloadDir) throws Exception {
        List<String> out = new ArrayList<>();
        if (dateStr == null || dateStr.isBlank() || hours <= 0) {
            return out;
        }

        Files.createDirectories(Paths.get(downloadDir));
        HttpClient client = HttpClient.newBuilder().build();
        LocalDate date = LocalDate.parse(dateStr);

        for (int hour = 0; hour < hours; hour++) {
            LocalDate d = date.plusDays(hour / 24);
            int h = hour % 24;
            String datePart = d.toString();
            String fileName = datePart + "-" + h + ".json.gz";
            String url = "https://data.gharchive.org/" + fileName;
            String outPath = Paths.get(downloadDir, fileName).toString();

            if (Files.exists(Paths.get(outPath))) {
                System.out.println("[download] skip existing " + outPath);
                out.add(outPath);
                continue;
            }

            System.out.println("[download] " + url + " -> " + outPath);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(url))
                    .header("User-Agent", "Mozilla/5.0")
                    .GET()
                    .build();
            HttpResponse<byte[]> resp = client.send(req, HttpResponse.BodyHandlers.ofByteArray());
            if (resp.statusCode() != 200) {
                throw new RuntimeException("Download failed with status " + resp.statusCode());
            }
            try (FileOutputStream fos = new FileOutputStream(outPath)) {
                fos.write(resp.body());
            }
            out.add(outPath);
        }
        return out;
    }

    private static List<String> resolveInputFiles(String[] globs) throws Exception {
        List<String> files = new ArrayList<>();
        for (String pattern : globs) {
            // Keep explicit globs simple for now; download paths cover primary use case.
            if (pattern != null && !pattern.isBlank()) {
                // No-op by design for now; fixed download set is used in this project baseline.
            }
        }
        Collections.sort(files);
        return files;
    }

    private static List<String> dedupeAndSort(List<String> files) {
        Set<String> seen = new HashSet<>(files);
        List<String> out = new ArrayList<>(seen);
        Collections.sort(out);
        return out;
    }

    private static Long getRepoId(JsonNode record) {
        JsonNode repo = record.get("repo");
        if (repo == null || repo.isNull()) return null;
        JsonNode id = repo.get("id");
        if (id == null || id.isNull()) return null;
        if (id.isIntegralNumber()) return id.asLong();
        if (id.isTextual()) {
            try {
                return Long.parseLong(id.asText());
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private static boolean validateRecord(JsonNode record, boolean strict) {
        if (record == null || !record.isObject()) return false;
        String id = text(record, "id");
        String type = text(record, "type");
        String createdAt = text(record, "created_at");
        Long repoId = getRepoId(record);

        if (id.isEmpty() || type.isEmpty() || createdAt.isEmpty() || repoId == null) {
            return false;
        }
        if (strict) {
            if (!createdAt.contains("T") || !createdAt.endsWith("Z")) return false;
            if (type.length() > 100) return false;
        }
        return true;
    }

    private static String text(JsonNode n, String key) {
        JsonNode v = n.get(key);
        if (v == null || !v.isTextual()) return "";
        return v.asText();
    }

    private static Map<String, AggValue> processFiles(List<String> inputFiles, boolean strict, Stats stats) throws Exception {
        Map<String, AggValue> aggregates = new HashMap<>();
        for (String filePath : inputFiles) {
            System.out.println("[process] " + filePath);
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                    new GZIPInputStream(Files.newInputStream(Paths.get(filePath))), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    stats.totalLines++;
                    line = line.trim();
                    if (line.isEmpty()) {
                        stats.invalidLines++;
                        continue;
                    }
                    JsonNode record;
                    try {
                        record = OBJECT_MAPPER.readTree(line);
                    } catch (Exception e) {
                        stats.invalidLines++;
                        continue;
                    }
                    if (!validateRecord(record, strict)) {
                        stats.invalidLines++;
                        continue;
                    }
                    String createdAt = text(record, "created_at");
                    String eventDay = createdAt.substring(0, 10);
                    String eventType = text(record, "type").trim().toLowerCase(Locale.ROOT);
                    Long repoId = getRepoId(record);
                    if (repoId == null) {
                        stats.invalidLines++;
                        continue;
                    }

                    stats.validLines++;
                    String key = eventDay + "|" + eventType;
                    AggValue v = aggregates.getOrDefault(key, new AggValue());
                    v.count += 1;
                    v.sumRepoId += repoId;
                    aggregates.put(key, v);
                }
            }
        }
        return aggregates;
    }

    private static List<Row> sortedRows(Map<String, AggValue> aggregates) {
        List<Row> rows = new ArrayList<>();
        for (Map.Entry<String, AggValue> e : aggregates.entrySet()) {
            String[] parts = e.getKey().split("\\|", 2);
            String eventDay = parts[0];
            String eventType = parts.length > 1 ? parts[1] : "";
            rows.add(new Row(eventDay, eventType, e.getValue().count, e.getValue().sumRepoId));
        }
        rows.sort(Comparator.comparing(Row::eventDay).thenComparing(Row::eventType));
        return rows;
    }

    @SuppressWarnings("deprecation")
    private static void writeParquet(String outputParquet, List<Row> rows) throws Exception {
        ensureParent(outputParquet);
        Files.deleteIfExists(Paths.get(outputParquet));
        String schemaJson = """
                {
                  "type":"record",
                  "name":"aggregate_row",
                  "fields":[
                    {"name":"event_day","type":"string"},
                    {"name":"event_type","type":"string"},
                    {"name":"record_count","type":"long"},
                    {"name":"sum_repo_id","type":"long"}
                  ]
                }
                """;
        Schema avroSchema = new Schema.Parser().parse(schemaJson);
        Configuration conf = new Configuration();

        try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(new Path(outputParquet))
                .withSchema(avroSchema)
                .withCompressionCodec(CompressionCodecName.ZSTD)
                .withConf(conf)
                .build()) {
            for (Row row : rows) {
                GenericRecord rec = new GenericData.Record(avroSchema);
                rec.put("event_day", row.eventDay());
                rec.put("event_type", row.eventType());
                rec.put("record_count", row.recordCount());
                rec.put("sum_repo_id", row.sumRepoId());
                writer.write(rec);
            }
        }
    }

    private static String writeChecksum(String outputChecksum, List<Row> rows) throws Exception {
        ensureParent(outputChecksum);
        Files.deleteIfExists(Paths.get(outputChecksum));
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        for (Row row : rows) {
            String line = row.eventDay() + "|" + row.eventType() + "|" + row.recordCount() + "|" + row.sumRepoId() + "\n";
            digest.update(line.getBytes(StandardCharsets.UTF_8));
        }
        StringBuilder sb = new StringBuilder();
        for (byte b : digest.digest()) {
            sb.append(String.format("%02x", b));
        }
        String checksum = sb.toString();
        try (BufferedWriter w = new BufferedWriter(new FileWriter(outputChecksum, StandardCharsets.UTF_8))) {
            w.write(checksum);
            w.write("\n");
        }
        return checksum;
    }
}
