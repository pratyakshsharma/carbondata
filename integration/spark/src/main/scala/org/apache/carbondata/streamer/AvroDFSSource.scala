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

import org.apache.avro.generic.GenericRecord
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.avro.{AvroDeserializer, SchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties

class AvroDFSSource(carbonTable: CarbonTable) extends Source {

  override
  def getStream(
      ssc: StreamingContext,
      sparkSession: SparkSession): CarbonDStream = {
    val dfsFilePath = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_DFS_INPUT_PATH)
    val value = ssc.fileStream(FileFactory.getUpdatedFilePath(dfsFilePath))
    CarbonDStream(sparkSession, value.asInstanceOf[DStream[Any]])
  }

  override
  def prepareDFAndMerge(inputStream: CarbonDStream): Unit = {
    val sparkDataTypes = SchemaConverters.toSqlType(schema).dataType.asInstanceOf[StructType]
//    val converter = new ReaderSchemaBasedKafkaDeserializer(schema)
//    val encoder = RowEncoder.apply(sparkDataTypes).resolveAndBind()
    // TODO: handle separately with fil schema as reader shema in deserializer
    inputStream.inputDStream.asInstanceOf[DStream[GenericRecord]].foreachRDD { rdd =>
      val rows = rdd.map { row =>
        genericRecordToRow(row, sparkDataTypes)
      }
      val targetDs = inputStream.sparkSession
        .read
        .format("carbondata")
        .load(carbonTable.getTablePath)
      val sourceDS = inputStream.sparkSession.createDataFrame(rows, sparkDataTypes)
      // TODO: get src schema and send for schema evolution or enforcement and send required info
      inputStream.performMergeOperation(targetDs,
        sourceDS,
        keyColumn,
        mergeOperationType)
    }
  }
}
