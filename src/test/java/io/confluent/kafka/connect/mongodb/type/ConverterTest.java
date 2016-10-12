package io.confluent.kafka.connect.mongodb.type;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.junit.Before;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class ConverterTest {
  Converter converter;

  @Before
  public void before() {
    this.converter = new Converter();
  }

  void assertBsonValue(final Object input, final BsonValue expected, final Schema schema) {
    final BsonValue actual = this.converter.bsonValue(schema, input);
    assertNotNull("actual should not be null.", actual);
    assertEquals(expected, actual);
  }


  @Test
  public void bsonValue_int8() {
    assertBsonValue(new Byte("12"), new BsonInt32(12), Schema.INT8_SCHEMA);
  }

  @Test
  public void bsonValue_int16() {
    assertBsonValue(new Short("12"), new BsonInt32(12), Schema.INT16_SCHEMA);
  }

  @Test
  public void bsonValue_int32() {
    assertBsonValue(new Integer("12"), new BsonInt32(12), Schema.INT32_SCHEMA);
  }

  @Test
  public void bsonValue_int64() {
    assertBsonValue(new Long("12"), new BsonInt64(12), Schema.INT64_SCHEMA);
  }

  @Test
  public void bsonDocument() {
    Map<String, Map.Entry<Schema, Object>> testData = new LinkedHashMap<>();
    testData.put("int8", new AbstractMap.SimpleEntry<>(Schema.INT8_SCHEMA, (Object) new Byte("12")));
    testData.put("int16", new AbstractMap.SimpleEntry<>(Schema.INT16_SCHEMA, (Object) new Short("12")));
    testData.put("int32", new AbstractMap.SimpleEntry<>(Schema.INT32_SCHEMA, (Object) new Integer("12")));
    testData.put("int64", new AbstractMap.SimpleEntry<>(Schema.INT64_SCHEMA, (Object) new Long("12")));
    testData.put("float32", new AbstractMap.SimpleEntry<>(Schema.FLOAT32_SCHEMA, (Object) new Float("12")));
    testData.put("float64", new AbstractMap.SimpleEntry<>(Schema.FLOAT64_SCHEMA, (Object) new Double("12")));

    testData.put("string", new AbstractMap.SimpleEntry<>(Schema.STRING_SCHEMA, (Object) "Testing"));

    testData.put("optional_int8", new AbstractMap.SimpleEntry<>(Schema.OPTIONAL_INT8_SCHEMA, null));
    testData.put("optional_int16", new AbstractMap.SimpleEntry<>(Schema.OPTIONAL_INT16_SCHEMA, null));
    testData.put("optional_int32", new AbstractMap.SimpleEntry<>(Schema.OPTIONAL_INT32_SCHEMA, null));
    testData.put("optional_int64", new AbstractMap.SimpleEntry<>(Schema.OPTIONAL_INT64_SCHEMA, null));
    testData.put("optional_string", new AbstractMap.SimpleEntry<>(Schema.OPTIONAL_STRING_SCHEMA, null));
    testData.put("optional_float32", new AbstractMap.SimpleEntry<>(Schema.OPTIONAL_FLOAT32_SCHEMA, null));
    testData.put("optional_float64", new AbstractMap.SimpleEntry<>(Schema.OPTIONAL_FLOAT64_SCHEMA, null));

    SchemaBuilder schemaBuilder = SchemaBuilder.struct();

    for (Map.Entry<String, Map.Entry<Schema, Object>> kvp : testData.entrySet()) {
      schemaBuilder.field(kvp.getKey(), kvp.getValue().getKey());
    }

    Schema schema = schemaBuilder.build();
    Struct struct = new Struct(schema);

    for (Map.Entry<String, Map.Entry<Schema, Object>> kvp : testData.entrySet()) {
      struct.put(kvp.getKey(), kvp.getValue().getValue());
    }

    SinkRecord sinkRecord = new SinkRecord(
        "testing",
        1,
        null,
        null,
        schema,
        struct,
        1234L
    );

    BsonDocument bsonDocument = this.converter.valueDocument(sinkRecord);

  }

}
