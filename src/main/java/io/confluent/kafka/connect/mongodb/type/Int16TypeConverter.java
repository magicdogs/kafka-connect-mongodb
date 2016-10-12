package io.confluent.kafka.connect.mongodb.type;

import org.apache.kafka.connect.data.Schema;
import org.bson.BsonInt32;
import org.bson.BsonValue;

class Int16TypeConverter implements TypeConverter {
  @Override
  public Schema schema() {
    return Schema.INT16_SCHEMA;
  }

  @Override
  public BsonValue bsonValue(Object input) {
    Short byteValue = (Short) input;
    int intValue = byteValue.intValue();
    return new BsonInt32(intValue);
  }
}
