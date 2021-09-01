/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.carbondata.streamer

import java.{lang, util}

import io.confluent.kafka.serializers.KafkaAvroDeserializer
import org.apache.avro.Schema

import org.apache.carbondata.core.constants.CarbonCommonConstants

/**
 * A kafka avro deserializer which has to cofigured when the kafka has avro records and the
 * schema is present in the schema registry.
 */
case class ReaderSchemaBasedKafkaDeserializer() extends KafkaAvroDeserializer {

  // read schema extracted from schema registry.
  var readSchema: Schema = _

  override def configure(configs: util.Map[String, _],
      isKey: Boolean): Unit = {
    super.configure(configs, isKey)
    readSchema = new Schema.Parser().parse(configs
      .get(CarbonCommonConstants.AVRO_SCHEMA)
      .asInstanceOf[String])
  }

  /**
   * Provide the read schema during deserialize, so that the schema evolution part is take care
   * and after deserialize data will be returned with the default values added to columns not
   * present in the actual data, but present in reader schema.
   */
  override def deserialize(includeSchemaAndVersion: Boolean,
      topic: String,
      isKey: lang.Boolean,
      payload: Array[Byte],
      readerSchema: Schema): AnyRef = {
    super.deserialize(
      includeSchemaAndVersion,
      topic,
      isKey,
      payload,
      readSchema)
  }
}
