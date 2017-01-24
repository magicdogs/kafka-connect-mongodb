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
package com.github.jcustenborder.kafka.connect.mongodb.type;

import com.google.common.base.Preconditions;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.BsonValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Converter {
  private static final Logger log = LoggerFactory.getLogger(Converter.class);
  final Map<SchemaKey, TypeConverter> converterLookup = new HashMap<>();

  public Converter() {
    registerTypeConverter(new Int8TypeConverter());
    registerTypeConverter(new Int16TypeConverter());
    registerTypeConverter(new Int32TypeConverter());
    registerTypeConverter(new Int64TypeConverter());
    registerTypeConverter(new StringTypeConverter());
    registerTypeConverter(new TimestampTypeConverter());
    registerTypeConverter(new DecimalTypeConverter());
    registerTypeConverter(new Float32TypeConverter());
    registerTypeConverter(new Float64TypeConverter());
    registerTypeConverter(new ArrayTypeConverter());
    registerTypeConverter(new MapTypeConverter());
  }

  public final void registerTypeConverter(TypeConverter typeConverter) {
    Preconditions.checkNotNull(typeConverter, "typeConverter cannot be null.");
    SchemaKey schemaKey = new SchemaKey(typeConverter.schema());
    TypeConverter existingTypeConverter = this.converterLookup.get(schemaKey);
    if (null != existingTypeConverter && log.isWarnEnabled()) {
      log.warn("Schema '{}' is already assigned to {}.", schemaKey, existingTypeConverter);
    }
    this.converterLookup.put(schemaKey, typeConverter);
  }

  public BsonValue bsonValue(Schema schema, Object value) {
    if (!schema.isOptional()) {
      Preconditions.checkNotNull(value, "value cannot be null.");
    } else {
      if (null == value) {
        return null;
      }
    }

    SchemaKey schemaKey = new SchemaKey(schema);
    TypeConverter typeConverter = this.converterLookup.get(schemaKey);
    if (null == typeConverter) {
      throw new UnsupportedOperationException(
          String.format(
              "Could not find TypeConverter for %s",
              schemaKey
          )
      );
    }
    return typeConverter.bsonValue(this, schema, value);
  }

  private BsonDocument document(Schema schema, Object value) {
    BsonDocument bsonDocument = new BsonDocument();
    Struct struct = (Struct) value;
    for (Field field : schema.fields()) {
      Object fieldValue = struct.get(field);
      try {
        BsonValue bsonValue = bsonValue(field.schema(), fieldValue);
        if (null != bsonValue) {
          bsonDocument.put(field.name(), bsonValue);
        }
      } catch (Exception ex) {
        throw new DataException(
            String.format("Exception thrown while processing field '%s'.", field.name()),
            ex
        );
      }
    }
    return bsonDocument;
  }


  public BsonDocument valueDocument(ConnectRecord record) {
    return document(record.valueSchema(), record.value());
  }

  public BsonDocument keyDocument(ConnectRecord record) {
    return document(record.keySchema(), record.key());
  }

  public SourceRecord sourceRecord(BsonDocument bsonDocument) {

    for (Map.Entry<String, BsonValue> kvp : bsonDocument.entrySet()) {


    }


    return null;
  }

}
