package io.confluent.kafka.connect.mongodb.type;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.bson.BsonArray;
import org.bson.BsonValue;

import java.util.List;

class ArrayTypeConverter implements TypeConverter {
  @Override
  public Schema schema() {
    return SchemaBuilder.array(Schema.STRING_SCHEMA).build();
  }

  @Override
  public BsonValue bsonValue(Converter converter, Schema schema, Object input) {
    List list = (List) input;

    BsonArray bsonArray = new BsonArray();
    for (Object o : list) {
      BsonValue bsonValue = converter.bsonValue(schema.valueSchema(), o);
      bsonArray.add(bsonValue);
    }

    return bsonArray;
  }
}
