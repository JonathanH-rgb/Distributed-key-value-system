package com.kvstore.common;

public class NodeInformation {

  public static enum Status {
    ALIVE,
    DEAD,
    SUSPECT,
  }

  private Status status;
  private long heartBeatCounter;
  private long incarnationNumber;

  public long getIncarnationNumber() {
    return incarnationNumber;
  }

  public void setIncarnationNumber(long incarnationNumber) {
    this.incarnationNumber = incarnationNumber;
  }

  public NodeInformation(Status status, long heartBeatCounter, long incarnationNumber) {
    this.status = status;
    this.heartBeatCounter = heartBeatCounter;
    this.incarnationNumber = incarnationNumber;
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
