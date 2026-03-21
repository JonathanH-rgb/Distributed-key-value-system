package com.kvstore.storage;

import java.util.Optional;

import com.kvstore.common.VersionedValue;

/**
 * Contract for the key-value storage layer.
 * Implementations are responsible for storing, retrieving, and deleting values
 * by key.
 */
public interface StorageEngine {

  Optional<VersionedValue> get(String key);

  void put(String key, byte[] value, long version);

  void delete(String key);
}
