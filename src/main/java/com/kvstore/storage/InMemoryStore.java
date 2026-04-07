package com.kvstore.storage;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.kvstore.common.VersionedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * In-memory implementation of StorageEngine.
 * Uses a ConcurrentHashMap to safely handle concurrent reads and writes from
 * multiple threads.
 */
public class InMemoryStore implements StorageEngine {

  private static final Logger logger = LoggerFactory.getLogger(InMemoryStore.class);

  private Map<String, VersionedValue> memoryStorage;

  public InMemoryStore() {
    this.memoryStorage = new ConcurrentHashMap<>();
  }

  public Optional<VersionedValue> get(String key) {
    logger.debug("Storage GET for key '{}'", key);
    return Optional.ofNullable(memoryStorage.get(key));
  }

  public void put(String key, byte[] value, long version) {
    logger.debug("Storage PUT for key '{}' at version {}", key, version);
    memoryStorage.put(key, new VersionedValue(value, version));
  }

  public void delete(String key) {
    logger.debug("Storage DELETE for key '{}'", key);
    memoryStorage.remove(key);
  }

}
