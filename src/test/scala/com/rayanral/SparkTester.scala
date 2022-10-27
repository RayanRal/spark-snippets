package com.rayanral

import org.apache.spark.sql._

trait SparkTester {

  val sparkSession: SparkSession = {
    SparkSession
      .builder()
      .master("local")
      .appName("test spark app")
      .getOrCreate()
  }
}