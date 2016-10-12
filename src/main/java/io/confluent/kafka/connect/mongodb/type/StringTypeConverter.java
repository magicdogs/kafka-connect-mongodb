package io.confluent.kafka.connect.mongodb.type;

import org.apache.kafka.connect.data.Schema;
import org.bson.BsonString;
import org.bson.BsonValue;

class StringTypeConverter implements TypeConverter {
  @Override
  public Schema schema() {
    return Schema.STRING_SCHEMA;
  }

  @Override
  public BsonValue bsonValue(Object input) {
    String stringValue = (String) input;
    return new BsonString(stringValue);
  }
}
