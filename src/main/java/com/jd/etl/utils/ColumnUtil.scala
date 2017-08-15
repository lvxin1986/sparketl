package com.jd.etl.utils

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataType

/**
  * Created by root on 17-8-15.
  */
object ColumnUtil {
  def castColumnTo( df: DataFrame, cn: String,tpe: DataType ) : DataFrame = {
    df.withColumn( cn, df.col(cn).cast(tpe))
  }

}
