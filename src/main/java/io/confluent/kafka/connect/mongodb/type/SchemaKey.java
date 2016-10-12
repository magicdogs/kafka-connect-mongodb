package io.confluent.kafka.connect.mongodb.type;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import org.apache.kafka.connect.data.Schema;

class SchemaKey implements Comparable<SchemaKey> {
  public final Schema.Type type;
  public final String logicalName;

  SchemaKey(Schema schema) {
    this(schema.type(), schema.name());
  }

  SchemaKey(Schema.Type type, String logicalName) {
    this.type = type;
    this.logicalName = logicalName == null ? "" : logicalName;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.type, this.logicalName);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("database", this.type)
        .add("collection", this.logicalName)
        .toString();
  }


  @Override
  public int compareTo(SchemaKey that) {
    if (null == that) {
      return 1;
    }
    return ComparisonChain.start()
        .compare(this.type, that.type)
        .compare(this.logicalName, that.logicalName)
        .result();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof SchemaKey) {
      return compareTo((SchemaKey) obj) == 0;
    } else {
      return false;
    }
  }
}
