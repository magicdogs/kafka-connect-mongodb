package io.confluent.kafka.connect.mongodb;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoDbSinkConnector extends SinkConnector {
  private static Logger log = LoggerFactory.getLogger(MongoDbSinkConnector.class);
  Map<String, String> settings;
  private MongoDbSinkConnectorConfig config;

  @Override
  public String version() {
    return VersionUtil.getVersion();
  }

  @Override
  public void start(Map<String, String> map) {
    this.settings = map;
    this.config = new MongoDbSinkConnectorConfig(map);
  }

  @Override
  public Class<? extends Task> taskClass() {
    return MongoDbSinkTask.class;
  }

  @Override
  public List<Map<String, String>> taskConfigs(int maxTasks) {
    List<Map<String, String>> taskConfigs = new ArrayList<>(maxTasks);

    for (int i = 0; i < maxTasks; i++) {
      taskConfigs.add(this.settings);
    }

    return taskConfigs;
  }

  @Override
  public void stop() {
  }

  @Override
  public ConfigDef config() {
    return MongoDbSinkConnectorConfig.getConfig();
  }
}
