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
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.execution.command.mutation.merge.CarbonMergeDataSetUtil
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties

/**
 * This class handles of preparing the Dstream and merging the data onto target carbondata table
 * for the kafka Source containing avro data.
 * @param carbonTable target carbondata table.
 */
class AvroKafkaSource(carbonTable: CarbonTable) extends Source with Serializable {

  override
  def getStream(
      ssc: StreamingContext,
      sparkSession: SparkSession): CarbonDStream = {
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
    inputStream.inputDStream.asInstanceOf[DStream[GenericRecord]].foreachRDD { rdd =>
      // Get the singleton instance of SparkSession
      val spark = SparkSession.builder.config(rdd.sparkContext.getConf).getOrCreate()
      val rowRDD = rdd.map { row =>
        genericRecordToRow(row, sparkDataTypes)
      }
      // TODO: check without alias and remove alias
      val targetDs = spark
        .sql(s"select * from ${ carbonTable.getTableName }")
        .as("A")
      val sourceDS = spark.createDataFrame(rowRDD, sparkDataTypes).as("B")
      CarbonMergeDataSetUtil.handleSchemaEvolutionForCarbonStreamer(targetDs, sourceDS, spark)
      val updatedTargetDs = spark
        .sql(s"select * from ${ carbonTable.getTableName }")
        .as("A")
      val updatedCarbonTable = CarbonEnv.getCarbonTable(Some(carbonTable.getDatabaseName),
        carbonTable.getTableName)(spark)
      val tableCols =
        updatedCarbonTable.getCreateOrderColumn.asScala.map(_.getColName).
          filterNot(_.equalsIgnoreCase(CarbonCommonConstants.DEFAULT_INVISIBLE_DUMMY_MEASURE))
      inputStream.performMergeOperation(updatedTargetDs,
        sourceDS.select(tableCols.map(col): _*).as("B"),
        keyColumn,
        mergeOperationType)
    }
  }

}
