package com.kvstore.common.exceptions;

public class WALCouldNotCloseLogFileException extends Exception {
  public WALCouldNotCloseLogFileException(String message) {
    super(message);
  }

  public WALCouldNotCloseLogFileException(String message, Throwable cause) {
    super(message, cause);
  }
}
