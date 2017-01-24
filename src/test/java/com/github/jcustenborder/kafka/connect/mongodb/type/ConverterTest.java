/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.mongodb.type;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.source.SourceRecord;
import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonInt64;
import org.bson.BsonValue;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;


public class ConverterTest {
  Converter converter;

  @BeforeEach
  public void before() {
    this.converter = new Converter();
  }


  static TestCase of(Object input, Object expected, Schema schema) {
    return new TestCase(input, expected, schema);
  }

  @TestFactory
  public Stream<DynamicTest> bsonValue() {
    List<TestCase> tests = Arrays.asList(
        of(new Long("12"), new BsonInt64(12), Schema.INT64_SCHEMA),
        of(new Byte("12"), new BsonInt32(12), Schema.INT8_SCHEMA),
        of(new Short("12"), new BsonInt32(12), Schema.INT16_SCHEMA),
        of(new Integer("12"), new BsonInt32(12), Schema.INT32_SCHEMA)
    );

    return tests.stream().map(testCase -> dynamicTest(testCase.toString(), () -> bsonValue(testCase)));
  }

  void bsonValue(TestCase testCase) {
    final BsonValue actual = this.converter.bsonValue(testCase.schema, testCase.input);
    assertNotNull(actual, "actual should not be null.");
    assertEquals(testCase.expected, actual);
  }

  @Disabled
  @Test
  public void bsonDocument() {
    Map<String, Map.Entry<Schema, Object>> testData = new LinkedHashMap<>();
    testData.put("int8", new AbstractMap.SimpleEntry<>(Schema.INT8_SCHEMA, (Object) new Byte("12")));
    testData.put("int16", new AbstractMap.SimpleEntry<>(Schema.INT16_SCHEMA, (Object) new Short("12")));
    testData.put("int32", new AbstractMap.SimpleEntry<>(Schema.INT32_SCHEMA, (Object) new Integer("12")));
    testData.put("int64", new AbstractMap.SimpleEntry<>(Schema.INT64_SCHEMA, (Object) new Long("12")));
    testData.put("float32", new AbstractMap.SimpleEntry<>(Schema.FLOAT32_SCHEMA, (Object) new Float("12")));
    testData.put("float64", new AbstractMap.SimpleEntry<>(Schema.FLOAT64_SCHEMA, (Object) new Double("12")));
    testData.put("timestamp", new AbstractMap.SimpleEntry<>(Timestamp.SCHEMA, (Object) new Date()));

    testData.put("array", new AbstractMap.SimpleEntry<>(SchemaBuilder.array(Schema.STRING_SCHEMA).build(), (Object) Arrays.asList("foo", "bar", "baz")));
    testData.put("map", new AbstractMap.SimpleEntry<>(SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA).build(), (Object) ImmutableMap.of("foo", "bar")));

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

    final ConnectRecord sinkRecord = new SinkRecord(
        "testing",
        1,
        null,
        null,
        schema,
        struct,
        1234L
    );

    final BsonDocument actualDocument = this.converter.valueDocument(sinkRecord);
    SourceRecord record = this.converter.sourceRecord(actualDocument);
    assertNotNull(record, "record cannot be null.");


  }

  static class TestCase {
    final Object input;
    final Object expected;
    final Schema schema;


    TestCase(Object input, Object expected, Schema schema) {
      this.input = input;
      this.expected = expected;
      this.schema = schema;
    }

    @Override
    public String toString() {
      return String.format("%s", this.expected.getClass().getSimpleName());
    }
  }
}
