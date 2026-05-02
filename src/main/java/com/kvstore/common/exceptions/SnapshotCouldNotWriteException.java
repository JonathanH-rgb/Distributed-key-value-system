package com.kvstore.common.exceptions;

public class SnapshotCouldNotWriteException extends Exception {
  public SnapshotCouldNotWriteException(String message) {
    super(message);
  }

  public SnapshotCouldNotWriteException(String message, Throwable cause) {
    super(message, cause);
  }
}
