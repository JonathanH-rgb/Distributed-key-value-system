package com.kvstore;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.kvstore.common.VersionedValue;
import com.kvstore.common.exceptions.StorageException;
import com.kvstore.storage.InMemoryStore;

public class InMemoryStoreTest {

  private InMemoryStore inMemoryStore;

  @BeforeEach
  public void setup() throws StorageException {
    inMemoryStore = new InMemoryStore();
    inMemoryStore.put("test1", "test1".getBytes(), 1L);
    inMemoryStore.put("test2", "test2".getBytes(), 1L);
  }

  @Test
  public void testGetOfExistingValue() throws StorageException {
    Optional<VersionedValue> optionalResult = inMemoryStore.get("test1");
    assertTrue(optionalResult.isPresent());
    assertArrayEquals("test1".getBytes(), optionalResult.get().getBytes());
  }

  @Test
  public void testGetOfNonExistingValue() throws StorageException {
    Optional<VersionedValue> result = inMemoryStore.get("thisDoesNotExist");
    assertTrue(result.isEmpty());
  }

  @Test
  public void testPutANewValue() throws StorageException {
    inMemoryStore.put("test3", "test3".getBytes(), 1L);
    Optional<VersionedValue> optionalResult = inMemoryStore.get("test3");
    assertTrue(optionalResult.isPresent());
    assertArrayEquals("test3".getBytes(), optionalResult.get().getBytes());
  }

  @Test
  public void testDeleteValue() throws StorageException {
    inMemoryStore.delete("test1");
    Optional<VersionedValue> result = inMemoryStore.get("test1");
    assertTrue(result.isEmpty());
  }

}
