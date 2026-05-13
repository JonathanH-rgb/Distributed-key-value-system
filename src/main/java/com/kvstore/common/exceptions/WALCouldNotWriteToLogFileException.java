package com.kvstore.common.exceptions;

public class WALCouldNotWriteToLogFileException extends Exception {
  public WALCouldNotWriteToLogFileException(String message) {
    super(message);
  }

  public WALCouldNotWriteToLogFileException(String message, Throwable cause) {
    super(message, cause);
  }
}
