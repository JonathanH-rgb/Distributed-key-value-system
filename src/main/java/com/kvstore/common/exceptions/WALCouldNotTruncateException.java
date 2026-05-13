package com.kvstore.common.exceptions;

public class WALCouldNotTruncateException extends Exception {
  public WALCouldNotTruncateException(String message) {
    super(message);
  }

  public WALCouldNotTruncateException(String message, Throwable cause) {
    super(message, cause);
  }
}
