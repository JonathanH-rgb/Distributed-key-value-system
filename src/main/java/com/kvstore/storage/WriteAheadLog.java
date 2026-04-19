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

import com.kvstore.common.exceptions.WALCouldNotOpenLogFileException;
import com.kvstore.common.exceptions.WALCouldNotWriteToLogFileException;
import com.kvstore.common.exceptions.WALEmptyWriteAttemptException;

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

  public void shutdown() throws IOException {
    writer.close();
  }

  // synchronized makes this only accesible from one thread at the time
  public synchronized void write(Operation operation, String key, byte[] value, long version)
      throws WALCouldNotWriteToLogFileException {
    long time = Instant.now().toEpochMilli();
    String valueString = Base64.getEncoder().encodeToString(value);
    String message = "";
    switch (operation) {
      case Operation.PUT:
        message = time + "|" + operation + "|" + key + "|" + valueString + "|" + version;
        break;
      case Operation.DELETE:
        message = time + "|" + operation + "|" + key;
        break;
    }

    if (message.isEmpty()) {
      // I think this should be a runtime exception, we can't recover from this logic
      throw new WALEmptyWriteAttemptException(
          "WAL produced empty message for operation=" + operation + " key=" + key + " version=" + version
              + " — unhandled Operation type, check the switch statement");
    }

    try {
      writer.write(message);
      writer.newLine();
      writer.flush();
      logger.debug("WAL wrote operation={} key={} version={}", operation, key, version);
    } catch (IOException ex) {
      throw new WALCouldNotWriteToLogFileException(
          "Failed to write to WAL file at " + logPath + " for operation=" + operation + " key=" + key, ex);
    }
  }

}
