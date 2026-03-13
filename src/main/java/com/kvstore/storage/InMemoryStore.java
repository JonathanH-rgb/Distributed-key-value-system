package com.kvstore.storage;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * In-memory implementation of StorageEngine.
 * Uses a ConcurrentHashMap to safely handle concurrent reads and writes from multiple threads.
 */
public class InMemoryStore implements StorageEngine {

  private Map<String, byte[]> memoryStorage;

  public InMemoryStore() {
    this.memoryStorage = new ConcurrentHashMap<>();
  }

  public Optional<byte[]> get(String key) {
    return Optional.ofNullable(memoryStorage.get(key));
  }

  public void put(String key, byte[] value) {
    memoryStorage.put(key, value);
  }

  public void delete(String key) {
    memoryStorage.remove(key);
  }

}
