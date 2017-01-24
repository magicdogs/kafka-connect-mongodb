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

import com.github.jcustenborder.kafka.connect.mongodb.type.Converter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.MultimapBuilder;
import com.mongodb.async.client.MongoClient;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.async.client.MongoClients;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import com.mongodb.bulk.BulkWriteResult;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.bson.BsonDocument;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MongoDbSinkTask extends SinkTask {
  private static Logger log = LoggerFactory.getLogger(MongoDbSinkTask.class);
  MongoDbSinkConnectorConfig config;
  MongoClient mongoClient;
  Converter converter = new Converter();
  CacheLoader<MongoCollectionKey, MongoCollection<BsonDocument>> mongoCollectionCacheLoader;
  LoadingCache<MongoCollectionKey, MongoCollection<BsonDocument>> mongoCollectionCache;
  CacheLoader<String, MongoDatabase> mongoDatabaseCacheLoader;
  LoadingCache<String, MongoDatabase> mongoDatabaseCache;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    this.config = new MongoDbSinkConnectorConfig(map);
    MongoClientSettings settings = this.config.settings();
    this.mongoClient = MongoClients.create(settings);

    this.mongoDatabaseCacheLoader = new MongoDatabaseCacheLoader(this.mongoClient);
    this.mongoDatabaseCache = CacheBuilder.newBuilder()
        .refreshAfterWrite(this.config.databaseCacheRefreshInterval, TimeUnit.SECONDS)
        .build(this.mongoDatabaseCacheLoader);

    this.mongoCollectionCacheLoader = new MongoCollectionCacheLoader(this.mongoDatabaseCache);
    this.mongoCollectionCache = CacheBuilder.newBuilder()
        .refreshAfterWrite(this.config.databaseCacheRefreshInterval, TimeUnit.SECONDS)
        .build(this.mongoCollectionCacheLoader);

    if (log.isInfoEnabled()) {
      log.info("\n" +
          " __      __      ___.       _________             .__          \n" +
          "/  \\    /  \\ ____\\_ |__    /   _____/ ____ _____  |  |   ____  \n" +
          "\\   \\/\\/   // __ \\| __ \\   \\_____  \\_/ ___\\\\__  \\ |  | _/ __ \\ \n" +
          " \\        /\\  ___/| \\_\\ \\  /        \\  \\___ / __ \\|  |_\\  ___/ \n" +
          "  \\__/\\  /  \\___  >___  / /_______  /\\___  >____  /____/\\___  >\n" +
          "       \\/       \\/    \\/          \\/     \\/     \\/          \\/ \n");
    }
  }


  @Override
  public void put(Collection<SinkRecord> records) {
    ListMultimap<MongoCollectionKey, UpdateOneModel<BsonDocument>> documents = MultimapBuilder.ListMultimapBuilder
        .hashKeys()
        .arrayListValues(this.config.collectionExpectedUpdates)
        .build();

    UpdateOptions updateOptions = new UpdateOptions()
        .upsert(true)
        .bypassDocumentValidation(false);

    for (SinkRecord sinkRecord : records) {
      MongoCollectionKey collectionKey = new MongoCollectionKey("database", "collection");
      BsonDocument valuesDocument = this.converter.valueDocument(sinkRecord);
      BsonDocument filterDocument = this.converter.keyDocument(sinkRecord);

      BsonDocument updateDocument = new BsonDocument();
      updateDocument.put("$set", valuesDocument);
      documents.put(collectionKey,
          new UpdateOneModel<BsonDocument>(
              filterDocument,
              updateDocument,
              updateOptions)
      );
    }

    BulkWriteOptions bulkWriteOptions = new BulkWriteOptions()
        .ordered(false);

    try {
      for (MongoCollectionKey mongoCollectionKey : documents.keySet()) {
        MongoCollection<BsonDocument> mongoCollection = this.mongoCollectionCache.get(mongoCollectionKey);
        List<UpdateOneModel<BsonDocument>> updates = documents.get(mongoCollectionKey);

        if (!updates.isEmpty()) {
          if (log.isInfoEnabled()) {
            log.info("Writing {} documents to {}", updates.size(), mongoCollectionKey);
          }

          SingleResultFuture<BulkWriteResult> future = new SingleResultFuture<>();
          mongoCollection.bulkWrite(updates, bulkWriteOptions, future);
          try {
            BulkWriteResult bulkWriteResult = future.get();
          } catch (ExecutionException ex0) {
            throw new RetriableException("Exception thrown while writing to MongoDB", ex0);
          }
        }
      }
    } catch (Throwable ex) {
      throw new RetriableException("Exception thrown while writing to MongoDB", ex);
    }
  }

  @Override
  public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

  }

  @Override
  public void stop() {
    this.mongoClient.close();
  }

}
