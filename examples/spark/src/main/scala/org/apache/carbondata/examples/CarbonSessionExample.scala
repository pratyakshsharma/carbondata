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

package org.apache.carbondata.examples

import java.io.File

import org.apache.log4j.PropertyConfigurator
import org.apache.spark.sql.{SaveMode, SparkSession}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.examples.util.ExampleUtils

object CarbonSessionExample {

  def main(args: Array[String]) {
    val rootPath = new File(this.getClass.getResource("/").getPath
                            + "../../../..").getCanonicalPath
    System.setProperty("path.target", s"$rootPath/examples/spark/target")
    // print profiler log to a separated file: target/profiler.log
    PropertyConfigurator.configure(
      s"$rootPath/examples/spark/src/main/resources/log4j.properties")

    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.ENABLE_QUERY_STATISTICS, "false")
    val spark = ExampleUtils.createCarbonSession("CarbonSessionExample")
    spark.sparkContext.setLogLevel("error")
    Seq(
      "stored as carbondata"
//      "using carbondata",
//      "stored by 'carbondata'",
//      "stored by 'org.apache.carbondata.format'"
    ).foreach { formatSyntax =>
      exampleBody(spark, formatSyntax)
    }
    spark.close()
  }

  def exampleBody(spark : SparkSession, formatSyntax: String = "stored as carbondata"): Unit = {

    import scala.util.Random

    import spark.implicits._
    val r = new Random()
    val df = spark.sparkContext.parallelize(1 to 2)
      .map(x => ("akash" + x, BigDecimal.apply(x % 60), 3000))
      .toDF("name", "age", "salary")
    df.write.format("avro").save("/home/root1/Projects/avrodata")

    val df2 = spark.sparkContext.parallelize(1 to 2)
      .map(x => ("chandler", BigDecimal.apply(x % 30), 3000))
      .toDF("name", "age", "salary")
    df2.write.format("avro").mode(SaveMode.Append).save("/home/root1/Projects/avrodata")


    val df3 = spark.sparkContext.parallelize(1 to 2)
      .map(x => ("akash", BigDecimal.apply(x % 60), 3000))
      .toDF("name", "age", "salary")
    df3.write.format("avro").mode(SaveMode.Append).save("/home/root1/Projects/avrodata")
  }
}
