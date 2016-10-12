package io.confluent.kafka.connect.mongodb.type;

import org.apache.kafka.connect.data.Schema;
import org.bson.BsonInt32;
import org.bson.BsonValue;

class Int8TypeConverter implements TypeConverter {

  @Override
  public Schema schema() {
    return Schema.INT8_SCHEMA;
  }

  @Override
  public BsonValue bsonValue(Object input) {
    Byte byteValue = (Byte) input;
    int intValue = byteValue.intValue();
    return new BsonInt32(intValue);
  }
}
