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
import org.apache.avro.mapred.AvroKey
import org.apache.avro.mapreduce.AvroKeyInputFormat
import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.avro.SchemaConverters
import org.apache.spark.sql.execution.command.mutation.merge.CarbonMergeDataSetUtil
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties

/**
 * This class handles of preparing the Dstream and merging the data onto target carbondata table
 * for the DFS Source containing avro data.
 * @param carbonTable target carbondata table.
 */
class AvroDFSSource(carbonTable: CarbonTable) extends Source with Serializable {

  override
  def getStream(
      ssc: StreamingContext,
      sparkSession: SparkSession): CarbonDStream = {
    val dfsFilePath = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_DFS_INPUT_PATH)
    // here set the reader schema in the hadoop conf so that the AvroKeyInputFormat will read
    // using the reader schema and populate the default values for the columns where data is not
    // present. This will help to apply the schema changes to target carbondata table.
    sparkSession.sparkContext.hadoopConfiguration.set("avro.schema.input.key", schema.toString())
    val value = ssc.fileStream[AvroKey[Any], NullWritable, AvroKeyInputFormat[Any]](FileFactory
      .getUpdatedFilePath(dfsFilePath))
      .map(rec => rec._1.datum().asInstanceOf[GenericRecord])
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
