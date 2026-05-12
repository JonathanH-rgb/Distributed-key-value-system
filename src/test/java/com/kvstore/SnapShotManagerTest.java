package com.kvstore;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.kvstore.common.VersionedValue;
import com.kvstore.common.exceptions.SnapshotCouldNotReadException;
import com.kvstore.common.exceptions.SnapshotCouldNotWriteException;
import com.kvstore.storage.SnapShotManager;

public class SnapShotManagerTest {

  private Path tempDir;
  private SnapShotManager snapShotManager;

  @BeforeEach
  public void setup() throws IOException {
    tempDir = Files.createTempDirectory("snapshot-test");
    snapShotManager = new SnapShotManager(tempDir.toString());
  }

  @AfterEach
  public void teardown() throws IOException {
    Files.walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(p -> p.toFile().delete());
  }

  @Test
  public void recoverShouldReturnEmptyMapWhenNoSnapshotExists()
      throws SnapshotCouldNotReadException, IOException {
    Map<String, VersionedValue> recovered = snapShotManager.recover();
    assertEquals(recovered.size(), 0);
  }

  @Test
  public void recoverShouldReturnSameMapThatWasSnapshotted()
      throws SnapshotCouldNotWriteException, SnapshotCouldNotReadException {
    Map<String, VersionedValue> expected = new HashMap<>();
    String key1 = "key1";
    VersionedValue value1 = new VersionedValue("value1".getBytes(), 1L);
    expected.put(key1, value1);
    String key2 = "key2";
    VersionedValue value2 = new VersionedValue("value2".getBytes(), 2L);
    expected.put(key2, value2);
    snapShotManager.snapshot(expected);
    Map<String, VersionedValue> recovered = snapShotManager.recover();
    assertEquals(recovered, expected);
  }

  @Test
  public void snapshotShouldOverwritePreviousSnapshot()
      throws SnapshotCouldNotWriteException, SnapshotCouldNotReadException {
    Map<String, VersionedValue> old = new HashMap<>();
    String key1 = "key1";
    VersionedValue value1 = new VersionedValue("value1".getBytes(), 1L);
    old.put(key1, value1);
    snapShotManager.snapshot(old);
    Map<String, VersionedValue> expected = new HashMap<>();
    String key2 = "key2";
    VersionedValue value2 = new VersionedValue("value2".getBytes(), 2L);
    expected.put(key2, value2);
    snapShotManager.snapshot(expected);
    Map<String, VersionedValue> recovered = snapShotManager.recover();
    assertEquals(recovered, expected);
  }

  @Test
  public void latestSnapshotTimeShouldReturnTimestampFromFile()
      throws SnapshotCouldNotWriteException, SnapshotCouldNotReadException {
    Map<String, VersionedValue> expected = new HashMap<>();
    String key2 = "key2";
    VersionedValue value2 = new VersionedValue("value2".getBytes(), 2L);
    expected.put(key2, value2);
    long before = Instant.now().toEpochMilli();
    snapShotManager.snapshot(expected);
    long after = Instant.now().toEpochMilli();
    long snapshotTime = snapShotManager.latestSnapshotTime();
    assertTrue(before <= snapshotTime && after >= snapshotTime);
  }

}
