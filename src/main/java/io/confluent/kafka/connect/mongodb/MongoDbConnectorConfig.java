package io.confluent.kafka.connect.mongodb;

import com.mongodb.ServerAddress;
import com.mongodb.async.client.MongoClientSettings;
import com.mongodb.connection.ClusterSettings;
import com.mongodb.connection.SslSettings;
import com.mongodb.connection.netty.NettyStreamFactoryFactory;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

abstract class MongoDbConnectorConfig extends AbstractConfig {
  public static final String HOSTS_CONF = "mongodb.hosts";
  public static final String SSL_ENABLED_CONF = "mongodb.ssl.enable";
  public static final String SSL_ALLOW_INVALID_HOSTNAMES_CONF = "mongodb.ssl.allow.invalid.hostnames";
  static final String HOSTS_DOC = "MongoDb hosts to connect to.";
  static final String SSL_ENABLED_DOC = "Flag to detemine if SSL configuration should be enabled.";
  static final String SSL_ALLOW_INVALID_HOSTNAMES_DOC = "Flag to determine if invalid hostnames should be allowed.";
  private static final Logger log = LoggerFactory.getLogger(MongoDbConnectorConfig.class);
  public final List<String> hosts;
  public final boolean sslEnabled;
  public final boolean sslAllowInvalidHostnames;

  public MongoDbConnectorConfig(ConfigDef definition, Map<?, ?> originals) {
    super(definition, originals);

    this.hosts = this.getList(HOSTS_CONF);
    this.sslEnabled = this.getBoolean(SSL_ENABLED_CONF);
    this.sslAllowInvalidHostnames = this.getBoolean(SSL_ALLOW_INVALID_HOSTNAMES_CONF);
  }

  static ConfigDef getConfig() {
    return new ConfigDef()
        .define(HOSTS_CONF, ConfigDef.Type.LIST, ConfigDef.Importance.HIGH, HOSTS_DOC)
        .define(SSL_ENABLED_CONF, ConfigDef.Type.BOOLEAN, true, ConfigDef.Importance.HIGH, SSL_ENABLED_DOC)
        .define(SSL_ALLOW_INVALID_HOSTNAMES_CONF, ConfigDef.Type.BOOLEAN, false, ConfigDef.Importance.HIGH, SSL_ALLOW_INVALID_HOSTNAMES_DOC)
        ;
  }

  protected abstract void settings(MongoClientSettings.Builder builder);

  public MongoClientSettings settings() {
    MongoClientSettings.Builder mongoClientSettingsBuilder = MongoClientSettings.builder();

    List<ServerAddress> serverAddresses = new ArrayList<>();
    for (String host : this.hosts) {
      serverAddresses.add(
          new ServerAddress(host)
      );
    }

    ClusterSettings clusterSettings = ClusterSettings.builder()
        .hosts(serverAddresses)
        .build();
    mongoClientSettingsBuilder.clusterSettings(clusterSettings);

    if (this.sslEnabled) {
      SslSettings sslSettings = SslSettings.builder()
          .enabled(this.sslEnabled)
          .invalidHostNameAllowed(this.sslAllowInvalidHostnames)
          .build();
      mongoClientSettingsBuilder.sslSettings(sslSettings);
      mongoClientSettingsBuilder.streamFactoryFactory(new NettyStreamFactoryFactory());
    }
    settings(mongoClientSettingsBuilder);
    return mongoClientSettingsBuilder.build();
  }

}
