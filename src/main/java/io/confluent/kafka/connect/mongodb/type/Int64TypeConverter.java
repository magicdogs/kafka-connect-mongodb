package io.confluent.kafka.connect.mongodb.type;

import org.apache.kafka.connect.data.Schema;
import org.bson.BsonInt64;
import org.bson.BsonValue;

class Int64TypeConverter implements TypeConverter {
  @Override
  public Schema schema() {
    return Schema.INT64_SCHEMA;
  }

  @Override
  public BsonValue bsonValue(Object input) {
    long longValue = (long) input;
    return new BsonInt64(longValue);
  }
}
