package com.kvstore.storage;

import java.util.Map;

import com.kvstore.common.VersionedValue;
import com.kvstore.common.exceptions.SnapshotCouldNotReadException;
import com.kvstore.common.exceptions.SnapshotCouldNotWriteException;

/**
 * Contract for snapshot management.
 */
public interface SnapShotManagerInterface {

  void snapshot(Map<String, VersionedValue> map) throws SnapshotCouldNotWriteException;

  Map<String, VersionedValue> recover() throws SnapshotCouldNotReadException;

  long latestSnapshotTime() throws SnapshotCouldNotReadException;

}
