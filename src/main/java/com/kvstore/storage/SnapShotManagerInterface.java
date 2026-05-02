package com.kvstore.storage;

import java.util.Map;

import com.kvstore.common.VersionedValue;
import com.kvstore.common.exceptions.SnapshotCouldNotReadException;
import com.kvstore.common.exceptions.SnapshotCouldNotWriteException;

/**
 * Contract for snapshot management.
 * Abstracts snapshot writing and recovery so implementations can be swapped in tests.
 */
public interface SnapShotManagerInterface {

  void snapshot(Map<String, VersionedValue> map) throws SnapshotCouldNotWriteException;

  Map<String, VersionedValue> recover() throws SnapshotCouldNotReadException;

}
