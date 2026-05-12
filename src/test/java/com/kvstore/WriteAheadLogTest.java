package com.kvstore;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.kvstore.common.VersionedValue;
import com.kvstore.common.exceptions.WALCouldNotOpenLogFileException;
import com.kvstore.common.exceptions.WALCouldNotReadLogFileException;
import com.kvstore.common.exceptions.WALCouldNotWriteToLogFileException;
import com.kvstore.storage.WriteAheadLog;

public class WriteAheadLogTest {

  private Path tempDir;
  private WriteAheadLog wal;

  @BeforeEach
  public void setup() throws IOException, WALCouldNotOpenLogFileException {
    tempDir = Files.createTempDirectory("wal-test");
    wal = new WriteAheadLog(tempDir.toString());
  }

  @AfterEach
  public void teardown() throws Exception {
    wal.shutdown();
    Files.walk(tempDir)
        .sorted(java.util.Comparator.reverseOrder())
        .forEach(p -> p.toFile().delete());
  }

  @Test
  public void recoverShouldApplyDeletesOnTopOfPuts()
      throws WALCouldNotWriteToLogFileException, WALCouldNotReadLogFileException {
    long after = 0L;
    String key1 = "key1";
    byte[] value1 = "value1".getBytes();
    long version1 = 1L;
    wal.writePut(key1, value1, version1);
    String key2 = "key2";
    byte[] value2 = "value2".getBytes();
    long version2 = 2L;
    wal.writePut(key2, value2, version2);
    wal.writeDelete(key1);
    Map<String, VersionedValue> result = wal.recover(after);
    assertEquals(result.size(), 1);
    assertTrue(result.containsKey(key2));
    assertArrayEquals(result.get(key2).getBytes(), value2);
    assertEquals(result.get(key2).getVersion(), version2);
  }

  @Test
  public void recoverWithSinceShouldSkipOlderEntries()
      throws WALCouldNotWriteToLogFileException, WALCouldNotReadLogFileException, InterruptedException {
    String key3 = "key3";
    byte[] value3 = "value3".getBytes();
    long version3 = 3L;
    wal.writePut(key3, value3, version3);
    long after = Instant.now().toEpochMilli();
    String key1 = "key1";
    byte[] value1 = "value1".getBytes();
    long version1 = 1L;
    wal.writePut(key1, value1, version1);
    String key2 = "key2";
    byte[] value2 = "value2".getBytes();
    long version2 = 2L;
    wal.writePut(key2, value2, version2);
    Map<String, VersionedValue> result = wal.recover(after);
    assertEquals(result.size(), 2);
    assertTrue(result.containsKey(key1));
    assertTrue(result.containsKey(key2));
    assertArrayEquals(result.get(key1).getBytes(), value1);
    assertArrayEquals(result.get(key2).getBytes(), value2);
    assertEquals(result.get(key1).getVersion(), version1);
    assertEquals(result.get(key2).getVersion(), version2);
    assertFalse(result.containsKey(key3));
  }

  @Test
  public void recoverWithSinceZeroShouldReturnAllEntries()
      throws WALCouldNotWriteToLogFileException, WALCouldNotReadLogFileException {
    long after = 0L;
    String key3 = "key3";
    byte[] value3 = "value3".getBytes();
    long version3 = 3L;
    wal.writePut(key3, value3, version3);
    String key1 = "key1";
    byte[] value1 = "value1".getBytes();
    long version1 = 1L;
    wal.writePut(key1, value1, version1);
    String key2 = "key2";
    byte[] value2 = "value2".getBytes();
    long version2 = 2L;
    wal.writePut(key2, value2, version2);
    Map<String, VersionedValue> result = wal.recover(after);
    assertEquals(result.size(), 3);
    assertTrue(result.containsKey(key1));
    assertTrue(result.containsKey(key2));
    assertTrue(result.containsKey(key3));
    assertArrayEquals(result.get(key1).getBytes(), value1);
    assertArrayEquals(result.get(key2).getBytes(), value2);
    assertArrayEquals(result.get(key3).getBytes(), value3);
    assertEquals(result.get(key1).getVersion(), version1);
    assertEquals(result.get(key2).getVersion(), version2);
    assertEquals(result.get(key3).getVersion(), version3);
  }

}
