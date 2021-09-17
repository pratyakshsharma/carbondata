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

import org.apache.spark.sql.SparkSession
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
    val value = ssc.fileStream(FileFactory.getUpdatedFilePath(dfsFilePath))
    CarbonDStream(sparkSession, value.asInstanceOf[DStream[Any]])
  }

  override
  def prepareDFAndMerge(inputStream: CarbonDStream): Unit = {
    prepareDSForAvroSourceAndMerge(inputStream, carbonTable)
  }
}
