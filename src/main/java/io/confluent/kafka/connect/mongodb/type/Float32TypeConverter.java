package io.confluent.kafka.connect.mongodb.type;

import org.apache.kafka.connect.data.Schema;
import org.bson.BsonDouble;
import org.bson.BsonValue;

class Float32TypeConverter implements TypeConverter {
  @Override
  public Schema schema() {
    return Schema.FLOAT32_SCHEMA;
  }

  @Override
  public BsonValue bsonValue(Object input) {
    Float floatValue = (Float) input;
    return new BsonDouble(floatValue.doubleValue());
  }
}
