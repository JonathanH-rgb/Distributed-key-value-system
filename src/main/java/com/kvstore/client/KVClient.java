package com.kvstore.client;

import java.util.Optional;

import com.google.protobuf.ByteString;
import com.kvstore.proto.KVStoreGrpc.KVStoreBlockingStub;
import com.kvstore.proto.KVStoreProto.DeleteRequest;
import com.kvstore.proto.KVStoreProto.GetRequest;
import com.kvstore.proto.KVStoreProto.GetResponse;
import com.kvstore.proto.KVStoreProto.PutRequest;
import com.kvstore.proto.KVStoreGrpc;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

public class KVClient {

  private ManagedChannel managedChannel;
  private KVStoreBlockingStub stub;

  public KVClient(String host, int portNumber) {

    managedChannel = ManagedChannelBuilder
        .forAddress(host, portNumber)
        .usePlaintext()
        .build();

    stub = KVStoreGrpc.newBlockingStub(managedChannel);

  }

  public KVClient(ManagedChannel managedChannel) {
    this.managedChannel = managedChannel;
    stub = KVStoreGrpc.newBlockingStub(managedChannel);
  }

  public Optional<byte[]> get(String key) {

    GetRequest getRequest = GetRequest
        .newBuilder()
        .setKey(key)
        .build();

    GetResponse getResponse = stub.get(getRequest);

    if (getResponse.getFound()) {
      return Optional.of(getResponse.getValue().toByteArray());
    } else {
      return Optional.empty();
    }

  }

  public void put(String key, byte[] value) {

    PutRequest putRequest = PutRequest
        .newBuilder()
        .setKey(key)
        .setValue(ByteString.copyFrom(value))
        .build();

    stub.put(putRequest);
  }

  public void delete(String key) {
    DeleteRequest deleteRequest = DeleteRequest
        .newBuilder()
        .setKey(key)
        .build();
    stub.delete(deleteRequest);
  }

  public void shutdown() {
    managedChannel.shutdown();
  }

}
