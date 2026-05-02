package com.kvstore.common.exceptions;

public class SnapshotCouldNotReadException extends Exception {
  public SnapshotCouldNotReadException(String message) {
    super(message);
  }

  public SnapshotCouldNotReadException(String message, Throwable cause) {
    super(message, cause);
  }
}
