package com.kvstore;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.kvstore.storage.InMemoryStore;

public class InMemoryStoreTest {

  private InMemoryStore inMemoryStore;

  public InMemoryStoreTest() {
    inMemoryStore = new InMemoryStore();
  }

  @BeforeEach
  public void setup() {
    inMemoryStore = new InMemoryStore();
    inMemoryStore.put("test1", "test1".getBytes());
    inMemoryStore.put("test2", "test2".getBytes());
  }

  @Test
  public void testGetOfExistingValue() {
    Optional<byte[]> optionalResult = inMemoryStore.get("test1");
    assertEquals(true, optionalResult.isPresent());
    assertArrayEquals("test1".getBytes(), optionalResult.get());
  }

  @Test
  public void testGetOfNonExistingValue() {
    Optional<byte[]> result = inMemoryStore.get("thisDoesNotExist");
    assertEquals(true, result.isEmpty());
  }

  @Test
  public void testPutANewValue() {
    inMemoryStore.put("test3", "test3".getBytes());
    Optional<byte[]> optionalResult = inMemoryStore.get("test3");
    assertEquals(true, optionalResult.isPresent());
    assertArrayEquals("test3".getBytes(), optionalResult.get());
  }

  @Test
  public void testDeletAValue() {
    inMemoryStore.delete("test1");
    Optional<byte[]> result = inMemoryStore.get("test1");
    assertEquals(true, result.isEmpty());

  }

}
