package com.kvstore.storage;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.kvstore.common.VersionedValue;

/**
 * In-memory implementation of StorageEngine.
 * Uses a ConcurrentHashMap to safely handle concurrent reads and writes from
 * multiple threads.
 */
public class InMemoryStore implements StorageEngine {

  private Map<String, VersionedValue> memoryStorage;

  public InMemoryStore() {
    this.memoryStorage = new ConcurrentHashMap<>();
  }

  public Optional<VersionedValue> get(String key) {
    return Optional.ofNullable(memoryStorage.get(key));
  }

  public void put(String key, byte[] value, long version) {
    memoryStorage.put(key, new VersionedValue(value, version));
  }

  public void delete(String key) {
    memoryStorage.remove(key);
  }

}
