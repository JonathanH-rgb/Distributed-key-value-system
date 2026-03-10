package com.kvstore.common.exceptions;

public class NodeAlreadyInRingException extends Exception {
  public NodeAlreadyInRingException(String message) {
    super(message);
  }
}
