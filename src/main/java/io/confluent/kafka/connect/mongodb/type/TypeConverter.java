package io.confluent.kafka.connect.mongodb.type;

import org.apache.kafka.connect.data.Schema;
import org.bson.BsonValue;

public interface TypeConverter {
  Schema schema();

  BsonValue bsonValue(Object input);
}
