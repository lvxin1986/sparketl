package com.jd.etl

import java.io.ByteArrayInputStream
import java.util.Properties

import com.jd.etl.consts.ETLConst
import org.apache.spark.sql.SparkSession

/**
  * Created by ninet on 17-8-21.
  */
class EtlConfig (workSpace: String, spark: SparkSession) {
    val properties = new Properties
    properties.load(new ByteArrayInputStream(spark.sparkContext.textFile(workSpace + ETLConst.ETL_PROPERTIES_FILE).collect().mkString("\n").getBytes()))

    val threadsNum = properties.getProperty(ETLConst.ETL_THREADS, ETLConst.ETL_THREADS_DEFAUL_VALUE).toInt
    val targetTable = properties.getProperty(ETLConst.ETL_TARGET_TABLE)
    val targetPartition = properties.getProperty(ETLConst.ETL_TARGET_PARTITION)
    val targetPath = properties.getProperty(ETLConst.ETL_TARGET_PATH)
    val orcCoalesce = properties.getProperty(ETLConst.ETL_COALESCE, ETLConst.ETL_COALESCE_DEFAULT_VALUE).toInt

    /*val sampleTable = properties.getProperty(ETLConst.ETL_SAMPLE_TABLE)
    val tmpDb = properties.getProperty(ETLConst.ETL_TEMP_DATABASE)
    val tmpPath = properties.getProperty(ETLConst.ETL_TEMP_PATH)

    val sqlColumns = properties.getProperty(ETLConst.ETL_SQL_COLUMNS)*/

}
