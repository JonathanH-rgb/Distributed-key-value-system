package com.kvstore.common.exceptions;

public class WALCouldNotReadLogFileException extends Exception {
  public WALCouldNotReadLogFileException(String message) {
    super(message);
  }

  public WALCouldNotReadLogFileException(String message, Throwable cause) {
    super(message, cause);
  }
}
