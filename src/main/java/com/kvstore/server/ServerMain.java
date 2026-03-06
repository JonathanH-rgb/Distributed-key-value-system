package com.kvstore.server;

public class ServerMain {

  public ServerMain() {

  }

  public static void main(String[] args) {
    KVServer kvServer = new KVServer();
    kvServer.start(8080);
  }

}
