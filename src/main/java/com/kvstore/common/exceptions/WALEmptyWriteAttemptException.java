package com.kvstore.common.exceptions;

public class WALEmptyWriteAttemptException extends RuntimeException {
  public WALEmptyWriteAttemptException(String message) {
    super(message);
  }

  public WALEmptyWriteAttemptException(String message, Throwable cause) {
    super(message, cause);
  }
}
