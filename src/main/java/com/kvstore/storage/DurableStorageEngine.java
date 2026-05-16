package com.kvstore.storage;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.kvstore.common.VersionedValue;
import com.kvstore.common.exceptions.SnapshotCouldNotReadException;
import com.kvstore.common.exceptions.SnapshotCouldNotWriteException;
import com.kvstore.common.exceptions.StorageException;
import com.kvstore.common.exceptions.WALException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Durable implementation of StorageEngine.
 * Writes every operation to the WAL before applying it to the in-memory map,
 * so state can be recovered by replaying the log after a crash.
 */
public class DurableStorageEngine implements StorageEngine {

  private static final Logger logger = LoggerFactory.getLogger(DurableStorageEngine.class);

  private static final int THREADS_RUNNING_SNAPSHOT = 1;
  private static final long SNAPSHOT_LOOP_DELAY_SECS = 0;
  private static final long SNAPSHOT_FREQ_SECS = 3600;

  private Map<String, VersionedValue> memoryStorage = new ConcurrentHashMap<>();
  private final WriteAheadLogInterface writeAheadLog;
  private final SnapShotManagerInterface snapShotManager;

  public DurableStorageEngine(WriteAheadLogInterface writeAheadLog, SnapShotManagerInterface snapShotManager) {
    this.writeAheadLog = writeAheadLog;
    this.snapShotManager = snapShotManager;
  }

  private void recover() throws StorageException {
    try {
      long latestSnapshotTime = snapShotManager.latestSnapshotTime();
      memoryStorage = snapShotManager.recover();
      Map<String, VersionedValue> walRemaining = writeAheadLog.recover(latestSnapshotTime);
      for (String key : walRemaining.keySet()) {
        memoryStorage.put(key, walRemaining.get(key));
      }
    } catch (SnapshotCouldNotReadException | WALException ex) {
      throw new StorageException("Failed to recover storage engine", ex);
    }
  }

  public void start()
      throws StorageException {
    recover();
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(THREADS_RUNNING_SNAPSHOT);
    scheduler.scheduleAtFixedRate(() -> {
      try {
        snapShotManager.snapshot(memoryStorage);
        writeAheadLog.truncate();
      } catch (SnapshotCouldNotWriteException | WALException ex) {
        logger.error("Snapshot failed ", ex);
      }
    }, SNAPSHOT_LOOP_DELAY_SECS,
        SNAPSHOT_FREQ_SECS,
        TimeUnit.SECONDS);
  }

  public Optional<VersionedValue> get(String key) {
    logger.debug("Storage GET for key '{}'", key);
    return Optional.ofNullable(memoryStorage.get(key));
  }

  public void put(String key, byte[] value, long version) throws StorageException {
    logger.debug("Storage PUT for key '{}' at version {}", key, version);
    try {
      writeAheadLog.writePut(key, value, version);
    } catch (Exception ex) {
      throw new StorageException("Failed to store key=" + key, ex);
    }
    memoryStorage.put(key, new VersionedValue(value, version));
  }

  public void delete(String key) throws StorageException {
    logger.debug("Storage DELETE for key '{}'", key);
    try {
      writeAheadLog.writeDelete(key);
    } catch (Exception ex) {
      throw new StorageException("Failed to delete key=" + key, ex);
    }
    memoryStorage.remove(key);
  }

}
