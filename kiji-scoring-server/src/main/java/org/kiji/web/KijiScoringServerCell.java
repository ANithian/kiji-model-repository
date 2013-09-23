/**
 * (c) Copyright 2013 WibiData, Inc.
 *
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.
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

package org.kiji.web;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecord;

import org.kiji.schema.impl.AvroCellEncoder;

/**
 * Models what a Kiji cell looks like when returned to the client. If the value set in the cell
 * is an Avro record, then the value will be serialized into a JSON string (so the client can
 * deserialize this back to an Avro object).
 */
@JsonPropertyOrder({ "timestamp", "value" })
public class KijiScoringServerCell {

  @JsonProperty("family")
  private String mFamily;

  @JsonProperty("qualifier")
  private String mQualifier;

  @JsonProperty("timestamp")
  private Long mTimestamp;

  @JsonProperty("value")
  private String mValue;

  @JsonProperty("schema")
  private String mSchema;

  /**
   * Constructs a KijiRestCell given a timestamp and value.
   *
   * @param family is the family of the cell.
   * @param qualifier is the qualifier of the cell.
   * @param timestamp is the timestamp of the cell.
   * @param value is the cell's value. If this is an Avro object, then it will be serialized
   *        to JSON and returned as a string literal containing JSON data.
   * @throws IOException if there is a problem serializing the value into JSON.
   */
  public KijiScoringServerCell(String family, String qualifier, Long timestamp, Object value)
      throws IOException {
    mTimestamp = timestamp;
    mValue = getJsonString(value);
    mSchema = getSchema(value).toString();
    mFamily = family;
    mQualifier = qualifier;
  }

  /** Dummy constructor for Jackson **/
  public KijiScoringServerCell() {

  }
  /**
   * Returns the underlying cell's timestamp.
   *
   * @return the underlying cell's timestamp
   */
  public Long getTimestamp() {
    return mTimestamp;
  }

  /**
   * Returns the underlying cell's column value.
   *
   * @return the underlying cell's column value
   */
  public Object getValue() {
    return mValue;
  }

  /**
   * Returns an encoded JSON string for the given Avro object.
   *
   * @param inputRecord is the record to encode
   * @return the JSON string representing this Avro object.
   *
   * @throws IOException if there is an error.
   */
  private static String getJsonString(Object inputRecord) throws IOException {

    String jsonString = null;

    if (inputRecord instanceof GenericContainer) {
      GenericContainer record = (GenericContainer) inputRecord;
      ByteArrayOutputStream os = new ByteArrayOutputStream();
      JsonEncoder encoder = EncoderFactory.get().jsonEncoder(record.getSchema(), os);
      DatumWriter<GenericContainer> writer = new GenericDatumWriter<GenericContainer>();
      if (record instanceof SpecificRecord) {
        writer = new SpecificDatumWriter<GenericContainer>();
      }

      writer.setSchema(record.getSchema());
      writer.write(record, encoder);
      encoder.flush();
      jsonString = new String(os.toByteArray(), Charset.forName("UTF-8"));
      os.close();
    } else {
      jsonString = inputRecord.toString();
    }

    return jsonString;
  }

  /**
   * Returns the Avro Schema for this value.
   *
   * @param value is the record whose schema to return
   * @return the Schema object of the input value.
   */
  private static Schema getSchema(final Object value) {
    if (value instanceof GenericContainer) {
      return ((GenericContainer) value).getSchema();
    }
    final Schema primitiveSchema = AvroCellEncoder.PRIMITIVE_SCHEMAS
        .get(value.getClass().getCanonicalName());
    if (null != primitiveSchema) {
      return primitiveSchema;
    }
    throw new RuntimeException(String.format("Unsupported output type found."
        + "Class: %s", value.getClass().getCanonicalName()));
  }
}
