package com.kvstore.common;

public class NodeInformation {

  public static enum Status {
    ALIVE,
    DEAD,
    SUSPECT,
  }

  private Status status;
  private long heartBeatCounter;

  public NodeInformation(Status status, long heartBeatCounter) {
    this.status = status;
    this.heartBeatCounter = heartBeatCounter;
  }

  public void setStatus(Status status) {
    this.status = status;
  }

  public void setHeartBeatCounter(long heartBeatCounter) {
    this.heartBeatCounter = heartBeatCounter;
  }

  public Status getStatus() {
    return status;
  }

  public long getHeartBeatCounter() {
    return heartBeatCounter;
  }
}
