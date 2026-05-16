package com.kvstore.common.exceptions;

public class WALException extends Exception {
  public WALException(String message) {
    super(message);
  }

  public WALException(String message, Throwable cause) {
    super(message, cause);
  }
}
