package com.kvstore.common;

import java.util.Objects;

public class Node {

  private String host;
  private int port;

  public Node() {
  }

  public Node(String host, int port) {
    this.host = host;
    this.port = port;
  }

  @Override
  public String toString() {
    return "Node [host=" + host + ", port=" + port + "]";
  }

  public void sethost(String host) {
    this.host = host;
  }

  public void setport(int port) {
    this.port = port;
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
