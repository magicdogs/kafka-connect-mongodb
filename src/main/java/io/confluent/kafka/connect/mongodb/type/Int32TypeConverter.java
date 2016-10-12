package io.confluent.kafka.connect.mongodb.type;

import org.apache.kafka.connect.data.Schema;
import org.bson.BsonInt32;
import org.bson.BsonValue;

class Int32TypeConverter implements TypeConverter {
  @Override
  public Schema schema() {
    return Schema.INT32_SCHEMA;
  }

  @Override
  public BsonValue bsonValue(Object input) {
    int intValue = (int) input;
    return new BsonInt32(intValue);
  }
}
