package com.ingest.data.common

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait SparkFixture {

  protected implicit val spark: SparkSession = {
    val builder = SparkSession.builder()
      .appName(getClass.getSimpleName)
      .config("spark.sql.crossJoin.enabled", "true")
    builder.master("local[*]").getOrCreate()
  }

  protected def sc: SparkContext = spark.sparkContext

}