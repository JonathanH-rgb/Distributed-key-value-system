package com.kvstore.server;

import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;

import com.google.protobuf.ByteString;
import com.kvstore.common.Node;
import com.kvstore.common.NodeInformation;
import com.kvstore.common.VersionedValue;
import com.kvstore.proto.KVStoreGrpc;
import com.kvstore.proto.KVStoreProto.DeleteRequest;
import com.kvstore.proto.KVStoreProto.DeleteResponse;
import com.kvstore.proto.KVStoreProto.GetRequest;
import com.kvstore.proto.KVStoreProto.GetResponse;
import com.kvstore.proto.KVStoreProto.PutRequest;
import com.kvstore.proto.KVStoreProto.PutResponse;
import com.kvstore.storage.InMemoryStore;
import com.kvstore.storage.StorageEngine;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;

/**
 * gRPC service implementation for the KV store.
 * Handles Get, Put, and Delete RPCs from clients and delegates to a
 * StorageEngine.
 */
public class KVServer extends KVStoreGrpc.KVStoreImplBase {

  private Server server;
  private HashMap<Node, NodeInformation> nodeMap;

  private StorageEngine storageEngine;

  public KVServer() {
    this.storageEngine = new InMemoryStore();
    nodeMap = new HashMap<>();
  }

  public void start(int portNumber) {
    server = ServerBuilder.forPort(portNumber)
        .addService(this)
        .build();
    try {
      server.start();
      server.awaitTermination();
    } catch (IOException ioEx) {
      throw new RuntimeException("Failed to start server on port " + portNumber, ioEx);
    } catch (InterruptedException iEx) {
      throw new RuntimeException("Server interrupted", iEx);
    }
  }

  @Override
  public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
    GetResponse getResponse;
    try {
      Optional<VersionedValue> optionalValue = storageEngine.get(request.getKey());
      if (optionalValue.isPresent()) {
        getResponse = GetResponse
            .newBuilder()
            .setKey(request.getKey())
            .setValue(ByteString.copyFrom(optionalValue.get().getBytes()))
            .setFound(true)
            .setVersion(optionalValue.get().getVersion())
            .build();
      } else {
        getResponse = GetResponse
            .newBuilder()
            .setKey(request.getKey())
            .setFound(false)
            .build();
      }
      responseObserver.onNext(getResponse);
      responseObserver.onCompleted();
    } catch (Exception ex) {
      responseObserver.onError(ex);
    }
  }

  @Override
  public void put(PutRequest request, StreamObserver<PutResponse> responseObserver) {
    try {
      storageEngine.put(request.getKey(), request.getValue().toByteArray(),
          request.getVersion());
      PutResponse putResponse = PutResponse.newBuilder()
          .setSuccessful(true)
          .build();
      responseObserver.onNext(putResponse);
      responseObserver.onCompleted();
    } catch (Exception ex) {
      responseObserver.onError(ex);
    }
  }

  @Override
  public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
    try {
      storageEngine.delete(request.getKey());
      DeleteResponse deleteResponse = DeleteResponse.newBuilder()
          .setSuccessful(true)
          .build();
      responseObserver.onNext(deleteResponse);
      responseObserver.onCompleted();
    } catch (Exception ex) {
      responseObserver.onError(ex);
    }

  }
}
