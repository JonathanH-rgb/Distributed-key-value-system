package com.kvstore.client;

import java.util.HashMap;
import java.util.stream.Collectors;

import com.kvstore.common.Node;
import com.kvstore.common.NodeInformation;
import com.kvstore.proto.KVStoreGrpc;
import com.kvstore.proto.KVStoreGrpc.KVStoreBlockingStub;
import com.kvstore.proto.KVStoreProto.GossipRequest;
import com.kvstore.proto.KVStoreProto.GossipResponse;
import com.kvstore.proto.KVStoreProto.NodeStatus;
import com.kvstore.proto.KVStoreProto.PingRequest;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class GossipClient {

  private ManagedChannel managedChannel;
  private KVStoreBlockingStub stub;

  public GossipClient(String host, int portNumber) {

    managedChannel = ManagedChannelBuilder
        .forAddress(host, portNumber)
        .usePlaintext()
        .build();

    stub = KVStoreGrpc.newBlockingStub(managedChannel);

  }

  public GossipClient(ManagedChannel managedChannel) {
    this.managedChannel = managedChannel;
    stub = KVStoreGrpc.newBlockingStub(managedChannel);
  }

  public void shutdown() {
    managedChannel.shutdown();
  }

  public boolean ping() {
    PingRequest pingRequest = PingRequest.newBuilder().build();
    try {
      stub.ping(pingRequest);
      return true;
    } catch (Exception ex) {
      return false;
    }
  }

  public HashMap<Node, NodeInformation> gossip(HashMap<Node, NodeInformation> nodeInformation) {

    GossipRequest gossipRequest = GossipRequest.newBuilder()
        .addAllNodes(nodeInformation.entrySet().stream()
            .map(entry -> com.kvstore.proto.KVStoreProto.NodeInformation.newBuilder()
                .setNode(com.kvstore.proto.KVStoreProto.Node.newBuilder()
                    .setHost(entry.getKey().gethost())
                    .setPort(entry.getKey().getport())
                    .build())
                .setStatus(com.kvstore.proto.KVStoreProto.NodeStatus.forNumber(entry.getValue().getStatus().ordinal()))
                .setHeartBeatCounter(entry.getValue().getHeartBeatCounter())
                .build())
            .collect(Collectors.toList()))
        .build();

    GossipResponse gossipResponse = stub.gossip(gossipRequest);
    HashMap<Node, NodeInformation> ans = new HashMap<>();

    gossipResponse.getNodesList().stream().forEach(protoNode -> {

      NodeInformation.Status status;
      if (protoNode.getStatus() == NodeStatus.ALIVE) {
        status = NodeInformation.Status.ALIVE;
      } else if (protoNode.getStatus() == NodeStatus.DEAD) {
        status = NodeInformation.Status.DEAD;
      } else {
        status = NodeInformation.Status.SUSPECT;
      }

      ans.put(
          new Node(protoNode.getNode().getHost(), protoNode.getNode().getPort()),
          new NodeInformation(status, protoNode.getHeartBeatCounter()));

    });

    return ans;

  }

}
