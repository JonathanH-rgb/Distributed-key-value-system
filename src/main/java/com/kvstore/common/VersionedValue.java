package com.kvstore.common;

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

}
