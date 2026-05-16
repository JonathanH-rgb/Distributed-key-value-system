package com.kvstore.storage;

import java.util.Map;

import com.kvstore.common.VersionedValue;
import com.kvstore.common.exceptions.WALException;

/**
 * Contract for a write-ahead log.
 */
public interface WriteAheadLogInterface {

  void writePut(String key, byte[] value, long version) throws WALException;

  void writeDelete(String key) throws WALException;

  Map<String, VersionedValue> recover(long since) throws WALException;

  void shutdown() throws WALException;

  void truncate() throws WALException;

}
