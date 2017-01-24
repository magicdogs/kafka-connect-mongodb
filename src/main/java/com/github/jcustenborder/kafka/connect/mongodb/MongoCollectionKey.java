/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.mongodb;

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
