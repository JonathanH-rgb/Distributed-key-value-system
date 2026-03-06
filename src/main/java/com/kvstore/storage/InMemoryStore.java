package com.kvstore.storage;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

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
