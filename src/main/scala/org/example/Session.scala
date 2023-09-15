package org.example

import org.apache.spark.sql.SparkSession

object Session {
  def getSparkSession: SparkSession = {
    val spark = SparkSession
      .builder
      .master("local")
      .appName("Visa")
      .getOrCreate()
    spark

  }
  }
