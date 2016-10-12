package io.confluent.kafka.connect.mongodb;

import com.google.common.collect.ImmutableMap;
import com.mongodb.async.client.MongoClientSettings;
import io.confluent.kafka.connect.utils.config.MarkdownFormatter;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertNotNull;

public class MongoDbSinkConnectorConfigTest {
  MongoDbSinkConnectorConfig config;

  @Test
  public void doc() {
    System.out.println(
        MarkdownFormatter.toMarkdown(MongoDbSinkConnectorConfig.getConfig())
    );
  }

  @Before
  public void before() {
    Map<String, String> settings = ImmutableMap.of(
        MongoDbConnectorConfig.HOSTS_CONF, "localhost"
    );
    this.config = new MongoDbSinkConnectorConfig(settings);
  }

  @Test
  public void settings() {
    MongoClientSettings settings = this.config.settings();
    assertNotNull("settings should not be null.", settings);
  }
}
