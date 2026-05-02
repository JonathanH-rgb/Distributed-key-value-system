package com.kvstore.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kvstore.common.VersionedValue;
import com.kvstore.common.exceptions.SnapshotCouldNotReadException;
import com.kvstore.common.exceptions.SnapshotCouldNotWriteException;

import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

/**
 * Manages periodic snapshots of the in-memory store to disk.
 * A snapshot captures the full state of the map at a point in time.
 * On recovery, the snapshot is loaded first, then only WAL entries newer
 * than the snapshot timestamp need to be replayed.
 */
public class SnapShotManager {
  private static final Logger logger = LoggerFactory.getLogger(SnapShotManager.class);
  private final Path tmpPath;
  private final Path persistedPath;
  public final static String TMP_FILE_NAME = "snapshot.tmp";
  public final static String PERSISTED_FILE_NAME = "snapshot.dat";

  public SnapShotManager(String fileDir) {
    if (fileDir.charAt(fileDir.length() - 1) == '/') {
      tmpPath = Path.of(fileDir + TMP_FILE_NAME);
      persistedPath = Path.of(fileDir + PERSISTED_FILE_NAME);
    } else {
      tmpPath = Path.of(fileDir + "/" + TMP_FILE_NAME);
      persistedPath = Path.of(fileDir + "/" + PERSISTED_FILE_NAME);
    }
  }

  public void snapshot(Map<String, VersionedValue> map) throws SnapshotCouldNotWriteException {
    logger.info("Starting snapshot of {} keys to {}", map.size(), tmpPath);
    try (BufferedWriter writer = Files.newBufferedWriter(tmpPath, StandardOpenOption.CREATE)) {
      long time = Instant.now().toEpochMilli();
      writer.write(Long.toString(time));
      writer.newLine();
      for (String key : map.keySet()) {
        VersionedValue value = map.get(key);
        String valueString = Base64.getEncoder().encodeToString(value.getBytes());
        String message = key + "|" + valueString + "|" + value.getVersion();
        writer.write(message);
        writer.newLine();
      }
      logger.info("Snapshot written to tmp file, {} keys", map.size());
    } catch (IOException ex) {
      throw new SnapshotCouldNotWriteException("Failed to write snapshot to " + tmpPath, ex);
    }
    try {
      Files.move(tmpPath, persistedPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException ex) {
      throw new SnapshotCouldNotWriteException("Failed to commit snapshot from " + tmpPath + " to " + persistedPath,
          ex);
    }
    logger.info("Snapshot committed to {}", persistedPath);
  }

  public Map<String, VersionedValue> recover() throws SnapshotCouldNotReadException {
    Map<String, VersionedValue> map = new ConcurrentHashMap<>();
    try (Stream<String> lines = Files.lines(persistedPath)) {
      lines.skip(1).forEach(line -> {
        String[] parts = line.split("\\|");
        String key = parts[0];
        byte[] value = Base64.getDecoder().decode(parts[1]);
        long version = Long.parseLong(parts[2]);
        map.put(key, new VersionedValue(value, version));
      });
    } catch (NoSuchFileException ex) {
      logger.info("No snapshot file found at {}, starting with empty state", persistedPath);
      return map;
    } catch (IOException ex) {
      throw new SnapshotCouldNotReadException("Failed to read snapshot from " + persistedPath, ex);
    }
    logger.info("Snapshot recovery complete; recovered {} keys from {}", map.size(), persistedPath);
    return map;
  }

}
