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

import com.beust.jcommander.JCommander
import org.apache.spark.sql.{CarbonEnv, SparkSession}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.strategy.CarbonPlanHelper
import org.apache.spark.streaming.{Seconds, StreamingContext}

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.core.util.path.CarbonTablePath

object CarbonDataStreamer {

  def createConfig(streamerConfig: CarbonStreamerConfig,
      args: Array[String]): Unit = {
    JCommander.newBuilder().addObject(streamerConfig).build().parse(args : _*)
  }

  def main(args: Array[String]): Unit = {

    // parse the incoming arguments and prepare the configurations
    val streamerConfigs = new CarbonStreamerConfig()
    createConfig(streamerConfigs, args)
    streamerConfigs.setConfigsToCarbonProperty(streamerConfigs)

    val spark = SparkSession
      .builder()
      .master(streamerConfigs.sparkMaster)
      .appName("CarbonData Streamer tool")
      .config("spark.sql.extensions", "org.apache.spark.sql.CarbonExtensions")
      .enableHiveSupport()
      .getOrCreate()
    CarbonEnv.getInstance(spark)

    SparkSession.setActiveSession(spark)
    SparkSession.setDefaultSession(spark)

    val batchDuration = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_BATCH_INTERVAL,
        CarbonCommonConstants.CARBON_STREAMER_BATCH_INTERVAL_DEFAULT).toLong
    val ssc = new StreamingContext(spark.sparkContext, Seconds(batchDuration))

    val targetTableName = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_TABLE_NAME)

    var databaseName = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_DATABASE_NAME)
    databaseName = if (databaseName.equalsIgnoreCase("")) {
      spark.sessionState.catalog.getCurrentDatabase
    } else {
      databaseName
    }

    // if the target table is non-carbondata table, throw exception
    if (!CarbonPlanHelper.isCarbonTable(TableIdentifier(targetTableName, Some(databaseName)))) {
      throw new UnsupportedOperationException("The merge operation using CarbonData Streamer tool" +
                                              " for non carbondata table is not supported.")
    }

    val targetCarbonDataTable = CarbonEnv.getCarbonTable(Some(databaseName), targetTableName)(spark)

    // set the checkpoint directory for spark streaming
    ssc.checkpoint(CarbonTablePath.getStreamingCheckpointDir(targetCarbonDataTable.getTablePath))

    val dbAndTb = databaseName + CarbonCommonConstants.POINT + targetTableName
    val segmentProperties = CarbonProperties.getInstance().getProperty(
      CarbonCommonConstants.CARBON_INPUT_SEGMENTS + dbAndTb, "")
    if (!(segmentProperties.equals("") || segmentProperties.trim.equals("*"))) {
      throw new MalformedCarbonCommandException(
        s"carbon.input.segments. $dbAndTb should not be set for table during merge operation. " +
        s"Please reset the property to carbon.input.segments.dbAndTb=*")
    }

    // prepare the target dataset based on target carbondata table.
    val targetDs = spark.read.format("carbondata").load(targetCarbonDataTable.getTablePath)
    val keyColumn = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_KEY_FIELD)
    val mergeOperationType = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_MERGE_OPERATION_TYPE,
        CarbonCommonConstants.CARBON_STREAMER_MERGE_OPERATION_TYPE_DEFAULT)

    // get the source Dstream based on source type
    val sourceType = CarbonProperties.getInstance()
      .getProperty(CarbonCommonConstants.CARBON_STREAMER_SOURCE_TYPE,
        CarbonCommonConstants.CARBON_STREAMER_SOURCE_TYPE_DEFAULT)
    val sourceCarbonDStream = SourceFactory.apply(sourceType, ssc, spark, targetCarbonDataTable)


    // Perform merge on source stream
    SourceFactory.source.prepareDFAndMerge(sourceCarbonDStream)

    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }
}
