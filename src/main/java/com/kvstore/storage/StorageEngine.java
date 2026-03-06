package com.kvstore.storage;

import java.util.Optional;

public interface StorageEngine {

  Optional<byte[]> get(String key);

  void put(String key, byte[] value);

  void delete(String key);
}
