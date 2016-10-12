package io.confluent.kafka.connect.mongodb;

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
