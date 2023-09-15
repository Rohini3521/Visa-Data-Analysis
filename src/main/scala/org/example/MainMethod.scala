package org.example

import org.apache.log4j.{Level, Logger}
import org.example.Input.getInputData
import org.example.Load.loadData
import org.example.Process.dataProcessing
import org.example.Session.getSparkSession

object MainMethod {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = getSparkSession

    val input_path = "D:/Visa/H-1B_Disclosure_Data_FY17.csv"
    var df = getInputData(input_path, spark)
    val processedDf = dataProcessing(df)


    val output_path = "D:/Visa/out.parquet"
    loadData(processedDf, output_path)


  }

}
