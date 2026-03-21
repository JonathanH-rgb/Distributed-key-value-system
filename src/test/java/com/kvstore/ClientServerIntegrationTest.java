package com.kvstore;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Optional;

import com.kvstore.common.VersionedValue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.kvstore.client.KVClient;
import com.kvstore.server.KVServer;

import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.inprocess.InProcessChannelBuilder;
import io.grpc.inprocess.InProcessServerBuilder;

public class ClientServerIntegrationTest {

  private KVClient client;
  private Server server;

  public ClientServerIntegrationTest() {
  }

  @BeforeEach
  public void setup() throws Exception {

    String serverName = "test-server";

    server = InProcessServerBuilder
        .forName(serverName)
        .addService(new KVServer())
        .build()
        .start();

    ManagedChannel managedChannel = InProcessChannelBuilder.forName(serverName)
        .build();

    client = new KVClient(managedChannel);

  }

  @AfterEach
  public void teardown() {
    client.shutdown();
    server.shutdown();
  }

  @Test
  public void testPutAndGet() {
    String key = "test";
    byte[] value = "value".getBytes();
    client.put(key, value, 1L);
    Optional<VersionedValue> gottenValue = client.get(key);
    assertTrue(gottenValue.isPresent());
    assertArrayEquals(value, gottenValue.get().getBytes());
  }

  @Test
  public void testGetNonExistingKey() {
    String key = "thisDoesNotExist";
    Optional<VersionedValue> gottenValue = client.get(key);
    assertFalse(gottenValue.isPresent());
  }

  @Test
  public void testDelete() {
    String key = "test";
    byte[] value = "value".getBytes();
    client.put(key, value, 1L);
    client.delete(key);
    Optional<VersionedValue> gottenValue = client.get(key);
    assertFalse(gottenValue.isPresent());
  }

}
