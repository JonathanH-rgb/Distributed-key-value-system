package com.kvstore.storage;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Base64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kvstore.common.exceptions.WALCouldNotCloseLogFileException;
import com.kvstore.common.exceptions.WALCouldNotOpenLogFileException;
import com.kvstore.common.exceptions.WALCouldNotWriteToLogFileException;

/**
 * Append-only log that records every write operation to disk before it is applied to memory.
 * On startup, the log can be replayed to recover state after a crash.
 */
public class WriteAheadLog {

  private static final Logger logger = LoggerFactory.getLogger(WriteAheadLog.class);

  public static enum Operation {
    PUT,
    DELETE
  }

  private final BufferedWriter writer;
  private final Path logPath;

  public WriteAheadLog(String fileDir) throws WALCouldNotOpenLogFileException {
    if (fileDir.charAt(fileDir.length() - 1) == '/') {
      logPath = Path.of(fileDir + "wal.log");
    } else {
      logPath = Path.of(fileDir + "/wal.log");
    }
    try {
      writer = Files.newBufferedWriter(logPath, StandardOpenOption.CREATE,
          StandardOpenOption.APPEND);
      logger.info("WAL opened at {}", logPath);
    } catch (IOException ex) {
      throw new WALCouldNotOpenLogFileException("Failed to open WAL file at " + logPath, ex);
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

}
