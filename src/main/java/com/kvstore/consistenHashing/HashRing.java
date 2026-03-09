package com.kvstore.consistenHashing;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

import com.kvstore.common.Node;

public class HashRing {

  private int virtualNodes;

  private Map<Long, VirtualNode> virtualNodeMap = new TreeMap<>();

  private Set<Node> nodesSet = new HashSet<>();

  private class VirtualNode {

    private Node nodeReference;
    private int index;

    public VirtualNode(Node nodeReference, int index) {
      this.nodeReference = nodeReference;
      this.index = index;
    }

    public void setindex(int index) {
      this.index = index;
    }

    public int getindex() {
      return index;
    }

    public Node getNodeReference() {
      return nodeReference;
    }

    public void setNodeReference(Node nodeReference) {
      this.nodeReference = nodeReference;
    }

    @Override
    public int hashCode() {
      return Objects.hash(nodeReference.gethost(), nodeReference.getport(), index);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      VirtualNode other = (VirtualNode) obj;
      if (!getEnclosingInstance().equals(other.getEnclosingInstance()))
        return false;
      if (nodeReference == null) {
        if (other.nodeReference != null)
          return false;
      } else if (nodeReference.getport() != other.nodeReference.getport() ||
          nodeReference.gethost().equals(other.nodeReference.gethost()))
        return false;
      if (index != other.index)
        return false;
      return true;
    }

    private HashRing getEnclosingInstance() {
      return HashRing.this;
    }

  }

  public HashRing(int virtualNodes) {
    if (virtualNodes <= 5) {
      throw new IllegalArgumentException("Please assign at least 5 virtual nodes");
    }
    this.virtualNodes = virtualNodes;
  }

  private long computeHashForRing(String arg) {
    // TODO : implement
    return 0L;
  }

  private String createVirtualNodeIdentifier(VirtualNode virtualNode) {
    return virtualNode.getNodeReference().gethost() +
        ":" +
        virtualNode.getNodeReference().getport() +
        "-" + virtualNode.getindex();
  }

  private void createVirtualNodes(Node node) {
    for (int i = 0; i < virtualNodes; i++) {
      VirtualNode virtualNode = new VirtualNode(node, i);
      String virtualNodeIdentifier = createVirtualNodeIdentifier(virtualNode);
      long virtualNodeHash = computeHashForRing(virtualNodeIdentifier);
      this.virtualNodeMap.put(virtualNodeHash, virtualNode);
    }
  }

  public void addNode(Node node) throws Exception {

    if (nodesSet.contains(node)) {
      throw new Exception("Node: " + node.toString() + "already in the ring");
    }

    try {
      createVirtualNodes(node);
      nodesSet.add(node);
    } catch (Exception ex) {

    } finally {

    }

  }

}
