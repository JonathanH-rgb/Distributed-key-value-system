package com.kvstore.common;

/**
 * Holds the gossip state for a single node in the cluster.
 * Tracks liveness status, heartbeat counter, and incarnation number.
 * Incarnation number is set to the node's startup timestamp and is used to
 * distinguish a restarted node from a stale entry with a higher heartbeat.
 */
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
