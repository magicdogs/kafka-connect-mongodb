package io.confluent.kafka.connect.mongodb.type;

import org.apache.kafka.connect.data.Schema;
import org.bson.BsonDouble;
import org.bson.BsonValue;

class Float64TypeConverter implements TypeConverter {
  @Override
  public Schema schema() {
    return Schema.FLOAT64_SCHEMA;
  }

  @Override
  public BsonValue bsonValue(Object input) {
    Double floatValue = (Double) input;
    return new BsonDouble(floatValue);
  }
}
