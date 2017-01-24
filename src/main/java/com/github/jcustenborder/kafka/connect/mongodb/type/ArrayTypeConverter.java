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
import org.bson.BsonArray;
import org.bson.BsonValue;

import java.util.List;

class ArrayTypeConverter implements TypeConverter {
  @Override
  public Schema schema() {
    return SchemaBuilder.array(Schema.STRING_SCHEMA).build();
  }

  @Override
  public BsonValue bsonValue(Converter converter, Schema schema, Object input) {
    List list = (List) input;

    BsonArray bsonArray = new BsonArray();
    for (Object o : list) {
      BsonValue bsonValue = converter.bsonValue(schema.valueSchema(), o);
      bsonArray.add(bsonValue);
    }

    return bsonArray;
  }

  @Override
  public Object connectValue(Converter converter, Schema schema, BsonValue input) {
    return null;
  }
}
