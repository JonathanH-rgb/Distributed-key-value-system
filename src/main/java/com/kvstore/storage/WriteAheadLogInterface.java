package com.kvstore.storage;

import java.util.Map;

import com.kvstore.common.VersionedValue;
import com.kvstore.common.exceptions.WALCouldNotCloseLogFileException;
import com.kvstore.common.exceptions.WALCouldNotReadLogFileException;
import com.kvstore.common.exceptions.WALCouldNotWriteToLogFileException;

/**
 * Contract for a write-ahead log.
 */
public interface WriteAheadLogInterface {

  void writePut(String key, byte[] value, long version) throws WALCouldNotWriteToLogFileException;

  void writeDelete(String key) throws WALCouldNotWriteToLogFileException;

  Map<String, VersionedValue> recover() throws WALCouldNotReadLogFileException;

  void shutdown() throws WALCouldNotCloseLogFileException;

}
