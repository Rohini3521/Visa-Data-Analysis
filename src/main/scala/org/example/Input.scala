package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}

object Input {
  def getInputData(input_path:String, spark:SparkSession): DataFrame ={

    val df = spark.read
      .option("header", "true")
      .format("csv")
      .load(input_path)

    df

  }

}
