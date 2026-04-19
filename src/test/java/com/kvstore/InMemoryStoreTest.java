package com.kvstore;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.kvstore.common.VersionedValue;
import com.kvstore.storage.DurableStorageEngine;

public class InMemoryStoreTest {

  private DurableStorageEngine inMemoryStore;

  @BeforeEach
  public void setup() {
    inMemoryStore = new DurableStorageEngine();
    inMemoryStore.put("test1", "test1".getBytes(), 1L);
    inMemoryStore.put("test2", "test2".getBytes(), 1L);
  }

  @Test
  public void testGetOfExistingValue() {
    Optional<VersionedValue> optionalResult = inMemoryStore.get("test1");
    assertTrue(optionalResult.isPresent());
    assertArrayEquals("test1".getBytes(), optionalResult.get().getBytes());
  }

  @Test
  public void testGetOfNonExistingValue() {
    Optional<VersionedValue> result = inMemoryStore.get("thisDoesNotExist");
    assertTrue(result.isEmpty());
  }

  @Test
  public void testPutANewValue() {
    inMemoryStore.put("test3", "test3".getBytes(), 1L);
    Optional<VersionedValue> optionalResult = inMemoryStore.get("test3");
    assertTrue(optionalResult.isPresent());
    assertArrayEquals("test3".getBytes(), optionalResult.get().getBytes());
  }

  @Test
  public void testDeleteValue() {
    inMemoryStore.delete("test1");
    Optional<VersionedValue> result = inMemoryStore.get("test1");
    assertTrue(result.isEmpty());
  }

}
