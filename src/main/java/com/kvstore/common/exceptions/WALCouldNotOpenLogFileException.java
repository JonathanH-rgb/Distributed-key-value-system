package com.kvstore.common.exceptions;

public class WALCouldNotOpenLogFileException extends Exception {
  public WALCouldNotOpenLogFileException(String message) {
    super(message);
  }

  public WALCouldNotOpenLogFileException(String message, Throwable cause) {
    super(message, cause);
  }
}
