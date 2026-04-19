package com.kvstore.storage;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.kvstore.common.VersionedValue;
import com.kvstore.common.exceptions.StorageException;
import com.kvstore.common.exceptions.WALCouldNotOpenLogFileException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory implementation of StorageEngine.
 * Uses a ConcurrentHashMap to safely handle concurrent reads and writes from
 * multiple threads.
 */
public class DurableStorageEngine implements StorageEngine {

  private static final Logger logger = LoggerFactory.getLogger(DurableStorageEngine.class);

  private Map<String, VersionedValue> memoryStorage = new ConcurrentHashMap<>();
  private WriteAheadLog writeAheadLog;

  public DurableStorageEngine() throws StorageException {
    try {
      writeAheadLog = new WriteAheadLog("a");
    } catch (WALCouldNotOpenLogFileException ex) {
      throw new StorageException("Failed to initialize storage engine", ex);
    }
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
