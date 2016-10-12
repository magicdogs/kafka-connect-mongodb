package io.confluent.kafka.connect.mongodb.type;

import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonValue;

import java.math.BigDecimal;

class DecimalTypeConverter implements TypeConverter {
  @Override
  public Schema schema() {
    return Decimal.schema(1);
  }

  @Override
  public BsonValue bsonValue(Object input) {
    BigDecimal decimal = (BigDecimal) input;
    BsonDocument bsonDocument = new BsonDocument();
    bsonDocument.put("unscaled", new BsonInt64(decimal.unscaledValue().longValue()));
    bsonDocument.put("scale", new BsonInt32(decimal.scale()));

    return bsonDocument;
  }
}
