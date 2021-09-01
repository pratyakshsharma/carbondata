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

import scala.collection.JavaConverters._

import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.StreamingContext

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

abstract class Source {

  protected var schema: Schema = _

  protected val keyColumn: String = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_STREAMER_KEY_FIELD)

  protected val mergeOperationType: String = CarbonProperties.getInstance()
    .getProperty(CarbonCommonConstants.CARBON_STREAMER_MERGE_OPERATION_TYPE)

  def loadSchemaBasedOnConfiguredClass(): Unit = {
    val schemaProviderClass = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_SCHEMA_PROVIDER,
        CarbonCommonConstants.CARBON_STREAMER_SCHEMA_PROVIDER_DEFAULT)
    val schemaSource = schemaProviderClass match {
      case "SchemaRegistry" | "org.apache.carbondata.streamer.SchemaRegistry" => SchemaRegistry
        .getClass
        .getClassLoader
        .loadClass("org.apache.carbondata.streamer.SchemaRegistry")
        .newInstance()
        .asInstanceOf[SchemaRegistry]
      case "FileSchema" | "org.apache.carbondata.streamer.FileSchema" => FileSchema
        .getClass
        .getClassLoader
        .loadClass("org.apache.carbondata.streamer.FileSchema")
        .newInstance()
        .asInstanceOf[FileSchema]
      case _ => throw new UnsupportedOperationException(
        "Schema provider other than SchemaRegistry and FileSchema are not supported. Please " +
        "configure the proper value.")
    }
    schema = schemaSource.getSchema
  }

  def genericRecordToRow(record: GenericRecord, sqlType : StructType): Row = {
    val objectArray = new Array[Any](record.asInstanceOf[GenericRecord].getSchema.getFields.size)
    for (field <- record.getSchema.getFields.asScala) {
      objectArray(field.pos) = record.get(field.pos)
    }

    new GenericRowWithSchema(objectArray, sqlType)
  }

  def getStream(
      ssc: StreamingContext,
      sparkSession: SparkSession): CarbonDStream

  def prepareDFAndMerge(inputStream: CarbonDStream)
}
