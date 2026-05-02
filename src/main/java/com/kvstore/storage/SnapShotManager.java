package com.kvstore.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.kvstore.common.VersionedValue;

import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.Base64;
import java.util.Map;

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

  private void shapshot(Map<String, VersionedValue> map) throws IOException {
    try (BufferedWriter writer = Files.newBufferedWriter(tmpPath, StandardOpenOption.CREATE)) {
      long time = Instant.now().toEpochMilli();
      // log here starting ...
      writer.write(Long.toString(time));
      writer.newLine();
      for (String key : map.keySet()) {
        VersionedValue value = map.get(key);
        String valueString = Base64.getEncoder().encodeToString(value.getBytes());
        String message = key + "|" + valueString + "|" + value.getVersion();
        writer.write(message);
        writer.newLine();
      }
    }
    // Log end here
    Files.move(tmpPath, persistedPath, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
    // Log shapshot sucessfull
  }

}
