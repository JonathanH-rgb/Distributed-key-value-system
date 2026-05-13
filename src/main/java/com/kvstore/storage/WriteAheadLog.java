package com.kvstore.storage;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kvstore.common.VersionedValue;
import com.kvstore.common.exceptions.WALCouldNotCloseLogFileException;
import com.kvstore.common.exceptions.WALCouldNotOpenLogFileException;
import com.kvstore.common.exceptions.WALCouldNotReadLogFileException;
import com.kvstore.common.exceptions.WALCouldNotTruncateException;
import com.kvstore.common.exceptions.WALCouldNotWriteToLogFileException;

/**
 * Append-only log that records every write operation to disk before it is
 * applied to memory.
 * On startup, the log can be replayed to recover state after a crash.
 */
public class WriteAheadLog implements WriteAheadLogInterface {

  private static final Logger logger = LoggerFactory.getLogger(WriteAheadLog.class);

  public static enum Operation {
    PUT,
    DELETE
  }

  private BufferedWriter writer;
  private final Path logPath;
  private final String fileDir;
  public final static String FILE_NAME = "wal.log";

  public WriteAheadLog(String fileDir) throws WALCouldNotOpenLogFileException {
    if (fileDir.charAt(fileDir.length() - 1) == '/') {
      this.fileDir = fileDir;
    } else {
      this.fileDir = fileDir + "/";
    }
    logPath = Path.of(fileDir + FILE_NAME);
    openWriter();
  }

  private void openWriter() throws WALCouldNotOpenLogFileException {
    try {
      writer = Files.newBufferedWriter(logPath, StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
      logger.info("WAL opened at {}", logPath);
    } catch (IOException ex) {
      throw new WALCouldNotOpenLogFileException("Failed to open WAL file at " + logPath, ex);
    }
  }

  public void truncate() throws WALCouldNotTruncateException {
    try {
      shutdown();
    } catch (WALCouldNotCloseLogFileException ex) {
      throw new WALCouldNotTruncateException("Failed to truncate WAL: could not close writer at " + logPath, ex);
    }
    try {
      Files.delete(logPath);
      logger.info("WAL truncated at {}", logPath);
    } catch (IOException ex) {
      throw new WALCouldNotTruncateException("Failed to truncate WAL: could not delete file at " + logPath, ex);
    }
    try {
      openWriter();
    } catch (WALCouldNotOpenLogFileException ex) {
      throw new WALCouldNotTruncateException("Failed to truncate WAL: could not reopen writer at " + logPath, ex);
    }
  }

  public void shutdown() throws WALCouldNotCloseLogFileException {
    try {
      writer.close();
      logger.info("WAL closed at {}", logPath);
    } catch (IOException ex) {
      throw new WALCouldNotCloseLogFileException("Failed to close WAL file at " + logPath, ex);
    }
  }

  // synchronized makes this only accesible from one thread at the time
  public synchronized void writePut(String key, byte[] value, long version)
      throws WALCouldNotWriteToLogFileException {
    long time = Instant.now().toEpochMilli();
    String valueString = Base64.getEncoder().encodeToString(value);
    String message = time + "|" + Operation.PUT + "|" + key + "|" + valueString + "|" + version;
    try {
      writer.write(message);
      writer.newLine();
      writer.flush();
      logger.debug("WAL wrote operation={} key={} version={}", Operation.PUT, key, version);
    } catch (IOException ex) {
      throw new WALCouldNotWriteToLogFileException(
          "Failed to write to WAL file at " + logPath + " for operation=" + Operation.PUT + " key=" + key, ex);
    }
  }

  // synchronized makes this only accesible from one thread at the time
  public synchronized void writeDelete(String key)
      throws WALCouldNotWriteToLogFileException {
    long time = Instant.now().toEpochMilli();
    String message = time + "|" + Operation.DELETE + "|" + key;
    try {
      writer.write(message);
      writer.newLine();
      writer.flush();
      logger.debug("WAL wrote operation={} key={}", Operation.DELETE, key);
    } catch (IOException ex) {
      throw new WALCouldNotWriteToLogFileException(
          "Failed to write to WAL file at " + logPath + " for operation=" + Operation.DELETE + " key=" + key, ex);
    }
  }

  public Map<String, VersionedValue> recover(long since) throws WALCouldNotReadLogFileException {
    Map<String, VersionedValue> memoryStorage = new ConcurrentHashMap<>();
    try (Stream<String> lines = Files.lines(logPath)) {
      lines.forEach(line -> {
        String[] parts = line.split("\\|");
        long time = Long.parseLong(parts[0]);
        if (time <= since) {
          return;
        }
        Operation operation = Operation.DELETE.toString().equals(parts[1]) ? Operation.DELETE : Operation.PUT;
        String key = parts[2];
        if (operation.equals(Operation.DELETE)) {
          memoryStorage.remove(key);
          logger.debug("WAL recovery: DELETE key={}", key);
        } else {
          byte[] value = Base64.getDecoder().decode(parts[3]);
          long version = Long.parseLong(parts[4]);
          memoryStorage.put(key, new VersionedValue(value, version));
          logger.debug("WAL recovery: PUT key={} version={}", key, version);
        }
      });
    } catch (IOException ex) {
      throw new WALCouldNotReadLogFileException("Failed to read WAL file at " + logPath, ex);
    }
    logger.info("WAL recovery complete; recovered {} keys from {}", memoryStorage.size(), logPath);
    return memoryStorage;
  }

}
