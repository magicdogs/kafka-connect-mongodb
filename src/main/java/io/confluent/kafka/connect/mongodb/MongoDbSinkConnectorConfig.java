package io.confluent.kafka.connect.mongodb;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.mongodb.WriteConcern;
import com.mongodb.async.client.MongoClientSettings;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;


class MongoDbSinkConnectorConfig extends MongoDbConnectorConfig {

  public static final String WRITE_CONCERN_CONF = "mongodb.write.concern";
  public static final String DATABASE_CACHE_REFRESH_INTERVAL_CONF = "mongodb.database.cache.refresh.interval.seconds";
  public static final String DATABASE_CACHE_REFRESH_INTERVAL_DOC = "The number of seconds to store the MongoDatabase and " +
      "MongoCollection objects in cache before refreshing them from the database.";
  public static final String COLLECTION_EXPECTED_UPDATES_CONF = "mongodb.collection.expected.updates";
  public static final String COLLECTION_EXPECTED_UPDATES_DOC = "The number of expected writes per collection.";
  static final String WRITE_CONCERN_DOC = "mongodb.write.concern";
  public final String writeConcern;
  public final long databaseCacheRefreshInterval;
  public final int collectionExpectedUpdates;

  public MongoDbSinkConnectorConfig(Map<String, String> parsedConfig) {
    super(getConfig(), parsedConfig);
    this.writeConcern = this.getString(WRITE_CONCERN_CONF);
    this.databaseCacheRefreshInterval = this.getLong(DATABASE_CACHE_REFRESH_INTERVAL_CONF);
    this.collectionExpectedUpdates = this.getInt(COLLECTION_EXPECTED_UPDATES_CONF);
  }

  static ConfigDef getConfig() {
    return MongoDbConnectorConfig.getConfig()
        .define(WRITE_CONCERN_CONF, Type.STRING, "ACKNOWLEDGED", new WriteConcernValidator(), Importance.MEDIUM, WRITE_CONCERN_DOC)
        .define(DATABASE_CACHE_REFRESH_INTERVAL_CONF, Type.LONG, 600L, ConfigDef.Range.atLeast(1), Importance.LOW, DATABASE_CACHE_REFRESH_INTERVAL_DOC)
        .define(COLLECTION_EXPECTED_UPDATES_CONF, Type.INT, 4096, ConfigDef.Range.atLeast(512), Importance.LOW, COLLECTION_EXPECTED_UPDATES_DOC)
        ;
  }

  @Override
  protected void settings(MongoClientSettings.Builder builder) {
    WriteConcern writeConcern = WriteConcern.valueOf(this.writeConcern);
    builder.writeConcern(writeConcern);
  }

  static class WriteConcernValidator implements ConfigDef.Validator {

    @Override
    public void ensureValid(String s, Object o) {
      Preconditions.checkState(o instanceof String, "'%s' should be a string", s);
      String value = (String) o;
      WriteConcern writeConcern = WriteConcern.valueOf(value);
      Preconditions.checkNotNull(writeConcern, "'%s' is not a valid WriteConcern", value);
    }

    @Override
    public String toString() {
      List<String> names = Lists.newArrayList();

      for (Field field : WriteConcern.class.getDeclaredFields()) {
        if (field.getType() == WriteConcern.class) {
          names.add(field.getName());
        }
      }
      Collections.sort(names);
      return Joiner.on(", ").join(names);
    }
  }
}
