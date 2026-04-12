package com.kvstore.common;

import java.util.Objects;

/**
 * Represents a cluster node identified by its host and port.
 * Equality is based on host and port so it can be used safely in maps and sets.
 */
public class Node {

  private final String host;
  private final int port;

  public Node(String host, int port) {
    this.host = host;
    this.port = port;
  }

  @Override
  public String toString() {
    return "Node [host=" + host + ", port=" + port + "]";
  }

  public String gethost() {
    return host;
  }

  public int getport() {
    return port;
  }

  @Override
  public int hashCode() {
    return Objects.hash(host, port);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null || getClass() != obj.getClass())
      return false;
    Node node = (Node) obj;
    return node.port == port && node.host.equals(host);
  }

}
