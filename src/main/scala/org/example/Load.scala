package org.example

import org.apache.spark.sql.DataFrame

object Load {
  def loadData(data: DataFrame, output_path: String): Unit = {
    if (!data.isEmpty) {
      print("Starting load_data :" + output_path)
      data.coalesce(1).write.mode("overwrite").parquet(output_path)
    }
    else {
      print("Empty dataframe, hence cannot save the data")
    }

  }


}
