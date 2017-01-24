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

import com.google.common.cache.CacheLoader;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoDatabase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class MongoDatabaseCacheLoader extends CacheLoader<String, MongoDatabase> {
  final static Logger log = LoggerFactory.getLogger(MongoDatabaseCacheLoader.class);
  final MongoClient mongoClient;

  public MongoDatabaseCacheLoader(MongoClient mongoClient) {
    this.mongoClient = mongoClient;
  }

  @Override
  public MongoDatabase load(String database) throws Exception {
    if (log.isDebugEnabled()) {
      log.debug("Looking up database '{}'", database);
    }
    return this.mongoClient.getDatabase(database);
  }
}
