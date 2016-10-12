package io.confluent.kafka.connect.mongodb.type;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Timestamp;
import org.bson.BsonDateTime;
import org.bson.BsonValue;

import java.util.Date;

class TimestampTypeConverter implements TypeConverter {
  @Override
  public Schema schema() {
    return Timestamp.SCHEMA;
  }

  @Override
  public BsonValue bsonValue(Converter converter, Schema schema, Object input) {
    long longValue = ((Date) input).getTime();
    return new BsonDateTime(longValue);
  }
}
