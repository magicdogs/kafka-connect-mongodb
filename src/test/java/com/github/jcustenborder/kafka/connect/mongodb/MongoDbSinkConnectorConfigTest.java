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

import com.google.common.collect.ImmutableMap;
import com.mongodb.async.client.MongoClientSettings;
import io.confluent.kafka.connect.utils.config.MarkdownFormatter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;


import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;


public class MongoDbSinkConnectorConfigTest {
  MongoDbSinkConnectorConfig config;

  @Test
  public void doc() {
    System.out.println(
        MarkdownFormatter.toMarkdown(MongoDbSinkConnectorConfig.getConfig())
    );
  }

  @BeforeEach
  public void before() {
    Map<String, String> settings = ImmutableMap.of(
        MongoDbConnectorConfig.HOSTS_CONF, "localhost"
    );
    this.config = new MongoDbSinkConnectorConfig(settings);
  }

  @Test
  public void settings() {
    MongoClientSettings settings = this.config.settings();
    assertNotNull(settings, "settings should not be null.");
  }
}
