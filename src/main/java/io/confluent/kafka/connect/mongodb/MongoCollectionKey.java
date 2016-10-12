package io.confluent.kafka.connect.mongodb;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

class MongoCollectionKey implements Comparable<MongoCollectionKey> {
  public final String database;
  public final String collection;

  MongoCollectionKey(String database, String collection) {
    this.database = database;
    this.collection = collection;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(this.database, this.collection);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("database", this.database)
        .add("collection", this.collection)
        .toString();
  }

  @Override
  public int compareTo(MongoCollectionKey that) {
    if (null == that) {
      return 1;
    }
    return ComparisonChain.start()
        .compare(this.database, that.database)
        .compare(this.collection, that.collection)
        .result();
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof MongoCollectionKey) {
      return compareTo((MongoCollectionKey) obj) == 0;
    } else {
      return false;
    }
  }
}
