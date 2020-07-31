package org.apache.druid.timeline.partition;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.hash.Hashing;
import org.apache.druid.java.util.common.StringUtils;

public enum HashPartitionFunction
{
  MURMUR3_32 {
    @Override
    int hash(byte[] bytes)
    {
      return Hashing.murmur3_32().hashBytes(bytes).asInt();
    }
  };

  abstract int hash(byte[] bytes);

  @JsonCreator
  public static HashPartitionFunction fromString(String type)
  {
    return HashPartitionFunction.valueOf(StringUtils.toUpperCase(type));
  }

  @Override
  @JsonValue
  public String toString()
  {
    return StringUtils.toLowerCase(name());
  }
}
