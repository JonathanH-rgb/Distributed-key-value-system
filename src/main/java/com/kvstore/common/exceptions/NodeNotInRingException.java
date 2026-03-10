package com.kvstore.common.exceptions;

public class NodeNotInRingException extends Exception {
  public NodeNotInRingException(String message) {
    super(message);
  }
}
