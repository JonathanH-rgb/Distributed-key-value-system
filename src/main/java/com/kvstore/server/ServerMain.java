package com.kvstore.server;

import com.kvstore.common.Node;
import com.kvstore.common.exceptions.EmptyHardcodedNodesListException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServerMain {

  private static final Logger logger = LoggerFactory.getLogger(ServerMain.class);

  public ServerMain() {
  }

  public static void main(String[] args) throws EmptyHardcodedNodesListException {
    logger.info("ServerMain starting up");
    Node[] hardcodedNodesInfo = new Node[] { new Node("host", 8080) };
    KVServer kvServer = new KVServer(hardcodedNodesInfo);
    kvServer.start(8088);
  }

}
