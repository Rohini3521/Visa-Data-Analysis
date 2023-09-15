package org.example

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{asc, col, count, when}
import org.apache.spark.sql.types.{DoubleType, IntegerType}

object Process {

  def dataProcessing(df: DataFrame): DataFrame = {

    var df1 = df.select("CASE_STATUS", "VISA_CLASS", "EMPLOYER_NAME",
      "JOB_TITLE", "PREVAILING_WAGE", "PW_SOURCE_YEAR", "WORKSITE_STATE")

    df1 = df1.withColumnRenamed("PREVAILING_WAGE", "SALARY")
      .withColumnRenamed("PW_SOURCE_YEAR", "FINANCIAL_YEAR").na.drop("all")
      .filter(col("CASE_STATUS") === "CERTIFIED")

    df1 = df1.withColumn("FINANCIAL_YEAR", col("FINANCIAL_YEAR").cast(IntegerType))
      .withColumn("SALARY", col("SALARY").cast(DoubleType)).na.drop(Seq("SALARY"))

    df1 = df1.filter(!col("EMPLOYER_NAME").endsWith("LLC"))

    var df2 = df1.filter(col("VISA_CLASS") === "H-1B")
      .groupBy("EMPLOYER_NAME").count().orderBy(col("count").desc)
      .withColumnRenamed("count", "APPROVED_VISA")

    val locationCountDF = df1.groupBy(col("WORKSITE_STATE"))
      .agg(
        count(when(col("SALARY") > 10 && col("SALARY") <= 10000, 1)).alias("Set1"),
        count(when(col("SALARY") > 10000 && col("SALARY") <= 50000, 1)).alias("Set2"),
        count(when(col("SALARY") > 50000 && col("SALARY") <= 100000, 1)).alias("Set3"),
        count(when(col("SALARY").isNotNull, 1)).alias("Others")
      ).select("WORKSITE_STATE", "Others", "Set1", "Set2", "Set3")
      .orderBy(asc("WORKSITE_STATE"))

    df1
  }

}
