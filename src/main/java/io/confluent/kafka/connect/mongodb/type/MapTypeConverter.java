package io.confluent.kafka.connect.mongodb.type;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.Map;

class MapTypeConverter implements TypeConverter {
  @Override
  public Schema schema() {
    return SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build();
  }

  @Override
  public BsonValue bsonValue(Converter converter, Schema schema, Object input) {
    Map map = (Map) input;

    if (Schema.Type.STRING != schema.keySchema().type()) {
      throw new UnsupportedOperationException(
          String.format(
              "Only keySchemas of String are supported. A key of '%s' is not supported by MongoDb.",
              schema.keySchema().type()
          )
      );
    }

    BsonDocument bsonDocument = new BsonDocument();
    for (Object key : map.keySet()) {
      String keyString = (String) key;
      Object value = map.get(key);
      BsonValue bsonValue = converter.bsonValue(schema.valueSchema(), value);
      bsonDocument.put(keyString, bsonValue);
    }
    return bsonDocument;
  }
}
