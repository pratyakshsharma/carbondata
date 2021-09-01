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

import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.avro.{AvroDeserializer, SchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties

class AvroKafkaSource(carbonTable: CarbonTable) extends Source {

  override
  def getStream(ssc: StreamingContext,
      sparkSession: SparkSession): CarbonDStream = {

    // TODO: check if we need to disable "enable.auto.commit" -> (false: java.lang.Boolean)

    // separate out the non carbon properties and prepare the kafka param
    val kafkaParams = CarbonProperties.getInstance()
      .getAllPropertiesInstance
      .asScala
      .filter { prop => !prop._1.startsWith("carbon") }
    kafkaParams.put(CarbonCommonConstants.AVRO_SCHEMA, schema.toString())
    val topics = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_KAFKA_INPUT_TOPIC)
      .split(CarbonCommonConstants.COMMA)
    val value = KafkaUtils
      .createDirectStream(ssc, PreferConsistent, Subscribe[String, String](topics, kafkaParams))
      .map(obj => obj.value().asInstanceOf[GenericRecord])
    CarbonDStream(sparkSession, value.asInstanceOf[DStream[Any]])
  }

  override
  def prepareDFAndMerge(inputStream: CarbonDStream): Unit = {
    val sparkDataTypes = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
//    val converter = new ReaderSchemaBasedKafkaDeserializer()
//    val encoder = RowEncoder.apply(sparkDataTypes).resolveAndBind()

    inputStream.inputDStream.asInstanceOf[DStream[GenericRecord]].foreachRDD { rdd =>
      val rowRDD = rdd.map { row =>
        genericRecordToRow(row, sparkDataTypes)
      }
      val targetDs = inputStream.sparkSession
        .read
        .format("carbondata")
        .load(carbonTable.getTablePath)
      val sourceDS = inputStream.sparkSession.createDataFrame(rowRDD, sparkDataTypes)
      // TODO: get src schema and send for schema evolution or enforcement and send required info
      // TODO: get the targetataset after schema evolution
      inputStream.performMergeOperation(targetDs,
        sourceDS,
        keyColumn,
        mergeOperationType)
    }
  }
}
