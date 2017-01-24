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
package com.github.jcustenborder.kafka.connect.mongodb.type;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.Map;

class MapTypeConverter implements TypeConverter {
  @Override
  public Schema schema() {
    return SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();
  }

  @Override
  public BsonValue bsonValue(Converter converter, Schema schema, Object input) {
    Map map = (Map) input;

    if (Schema.Type.STRING != schema.keySchema().type()) {
      throw new UnsupportedOperationException(
          String.format(
              "Only keySchemas of String are supported. A key of '%s' is not supported by MongoDb.",
              schema.keySchema().type()
          )
      );
    }

    BsonDocument bsonDocument = new BsonDocument();
    for (Object key : map.keySet()) {
      String keyString = (String) key;
      Object value = map.get(key);
      BsonValue bsonValue = converter.bsonValue(schema.valueSchema(), value);
      bsonDocument.put(keyString, bsonValue);
    }
    return bsonDocument;
  }

  @Override
  public Object connectValue(Converter converter, Schema schema, BsonValue input) {
    return null;
  }
}
