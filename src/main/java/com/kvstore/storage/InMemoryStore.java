package com.kvstore.storage;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import com.kvstore.common.VersionedValue;

/**
 * In-memory only implementation of StorageEngine with no durability.
 */
public class InMemoryStore implements StorageEngine {

  private Map<String, VersionedValue> memoryStorage = new ConcurrentHashMap<>();

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
