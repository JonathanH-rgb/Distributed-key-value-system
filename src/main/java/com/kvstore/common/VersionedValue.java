package com.kvstore.common;

import java.util.Arrays;
import java.util.Objects;

public class VersionedValue {

  private byte[] bytes;
  private long version;

  public VersionedValue(byte[] bytes, long version) {
    this.bytes = bytes.clone();
    this.version = version;
  }

  public void setVersion(long version) {
    this.version = version;
  }

  public long getVersion() {
    return version;
  }

  public byte[] getBytes() {
    return bytes.clone();
  }

  public void setBytes(byte[] bytes) {
    this.bytes = bytes.clone();
  }

  @Override
  public int hashCode() {
    return Objects.hash(version, Arrays.hashCode(bytes));
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    VersionedValue versionedValue = (VersionedValue) obj;
    return version == versionedValue.version && Arrays.equals(bytes, versionedValue.bytes);
  }

}
