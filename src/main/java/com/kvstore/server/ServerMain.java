package com.kvstore.server;

import com.kvstore.common.Node;
import com.kvstore.common.exceptions.EmptyHardcodedNodesListException;

public class ServerMain {

  public ServerMain() {
  }

  public static void main(String[] args) throws EmptyHardcodedNodesListException {
    Node[] hardcodedNodesInfo = new Node[] { new Node("host", 8080) };
    KVServer kvServer = new KVServer(hardcodedNodesInfo);
    kvServer.start(8088);
  }

}
