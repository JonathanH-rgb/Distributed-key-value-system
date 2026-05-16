package com.kvstore.common;

/**
 * Holds the gossip state for a single node in the cluster.
 * Tracks liveness status, heartbeat counter, and incarnation number.
 * Incarnation number is set to the node's startup timestamp and is used to
 * distinguish a restarted node from a stale entry with a higher heartbeat.
 *
 * Immutable: every state change produces a new instance. This prevents
 * concurrent updates from silently overwriting each other.
 */
public class NodeInformation {

  public enum Status {
    ALIVE,
    DEAD,
    SUSPECT,
  }

  private final Status status;
  private final long heartBeatCounter;
  private final long incarnationNumber;

  public NodeInformation(Status status, long heartBeatCounter, long incarnationNumber) {
    this.status = status;
    this.heartBeatCounter = heartBeatCounter;
    this.incarnationNumber = incarnationNumber;
  }

  public Status getStatus() {
    return status;
  }

  public long getHeartBeatCounter() {
    return heartBeatCounter;
  }

  public long getIncarnationNumber() {
    return incarnationNumber;
  }

  public NodeInformation withStatus(Status newStatus) {
    return new NodeInformation(newStatus, this.heartBeatCounter, this.incarnationNumber);
  }

  public NodeInformation withHeartBeatCounter(long newCounter) {
    return new NodeInformation(this.status, newCounter, this.incarnationNumber);
  }

}
