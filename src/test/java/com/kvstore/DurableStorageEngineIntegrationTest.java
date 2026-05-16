package com.kvstore;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Optional;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.kvstore.common.VersionedValue;
import com.kvstore.common.exceptions.StorageException;
import com.kvstore.common.exceptions.WALException;
import com.kvstore.storage.DurableStorageEngine;
import com.kvstore.storage.SnapShotManager;
import com.kvstore.storage.WriteAheadLog;

public class DurableStorageEngineIntegrationTest {

  private Path tempDir;
  private DurableStorageEngine engine;

  @BeforeEach
  public void setup() throws IOException, WALException {
    tempDir = Files.createTempDirectory("durable-engine-test");
    engine = new DurableStorageEngine(
        new WriteAheadLog(tempDir.toString()),
        new SnapShotManager(tempDir.toString()));
  }

  @AfterEach
  public void teardown() throws IOException {
    Files.walk(tempDir)
        .sorted(Comparator.reverseOrder())
        .forEach(p -> p.toFile().delete());
  }

  @Test
  public void keysShouldSurviveRestart()
      throws StorageException, WALException, IOException, InterruptedException {
    String key1 = "key1";
    byte[] value1 = "value1".getBytes();
    long version1 = 1L;
    engine.put(key1, value1, version1);
    engine.start();
    Thread.sleep(1000);
    String key2 = "key2";
    byte[] value2 = "value2".getBytes();
    long version2 = 2L;
    engine.put(key2, value2, version2);
    DurableStorageEngine newEngine = new DurableStorageEngine(
        new WriteAheadLog(tempDir.toString()),
        new SnapShotManager(tempDir.toString()));
    newEngine.start();
    Thread.sleep(1000);
    Optional<VersionedValue> result1 = newEngine.get(key1);
    assertTrue(result1.isPresent());
    assertArrayEquals(result1.get().getBytes(), value1);
    assertEquals(result1.get().getVersion(), version1);
    Optional<VersionedValue> result2 = newEngine.get(key2);
    assertTrue(result2.isPresent());
    assertArrayEquals(result2.get().getBytes(), value2);
    assertEquals(result2.get().getVersion(), version2);
  }

  @Test
  public void deleteShouldSurviveRestart()
      throws StorageException, WALException, InterruptedException {
    String key = "key";
    byte[] value = "value".getBytes();
    long version = 1L;
    engine.put(key, value, version);
    engine.delete(key);
    engine.start();
    Thread.sleep(1000);
    DurableStorageEngine newEngine = new DurableStorageEngine(
        new WriteAheadLog(tempDir.toString()),
        new SnapShotManager(tempDir.toString()));
    newEngine.start();
    Thread.sleep(1000);
    Optional<VersionedValue> result = newEngine.get(key);
    assertTrue(result.isEmpty());
  }
}
