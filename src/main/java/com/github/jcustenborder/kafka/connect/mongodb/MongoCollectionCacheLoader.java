/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
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

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MongoCollectionCacheLoader extends CacheLoader<MongoCollectionKey, MongoCollection<BsonDocument>> {
  final static Logger log = LoggerFactory.getLogger(MongoCollectionCacheLoader.class);
  final LoadingCache<String, MongoDatabase> mongoDatabaseCache;

  public MongoCollectionCacheLoader(LoadingCache<String, MongoDatabase> mongoDatabaseCache) {
    this.mongoDatabaseCache = mongoDatabaseCache;
  }

  @Override
  public MongoCollection<BsonDocument> load(MongoCollectionKey mongoCollectionKey) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("Loading Collection %s", mongoCollectionKey);
    }
    MongoDatabase mongoDatabase = this.mongoDatabaseCache.get(mongoCollectionKey.database);
    Preconditions.checkNotNull(mongoDatabase, "MongoDatabase('%s') was not found.", mongoCollectionKey.database);

    MongoCollection<BsonDocument> collection = mongoDatabase.getCollection(mongoCollectionKey.collection, BsonDocument.class);
    return collection;
  }
}
