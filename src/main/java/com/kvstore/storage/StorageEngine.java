package com.kvstore.storage;

import java.util.Optional;

/**
 * Contract for the key-value storage layer.
 * Implementations are responsible for storing, retrieving, and deleting values by key.
 */
public interface StorageEngine {

  Optional<byte[]> get(String key);

  void put(String key, byte[] value);

  void delete(String key);
}
