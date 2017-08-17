package com.jd.etl

import java.io.ByteArrayInputStream
import java.lang.Exception
import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.concurrent.Executors

import com.jd.etl.consts.ETLConst
import com.jd.etl.utils.ColumnUtil
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Created by root on 17-8-1.
  */
object Mysql2OrcParall {
  val hadoopConf = new org.apache.hadoop.conf.Configuration()
  val fs = org.apache.hadoop.fs.FileSystem.get(hadoopConf)

  def main(args: Array[String]) {
    if (args.size == 1) {
      InitData(args(0))
    } else {
      ImportDataDaily(args(0), args(1), args(2), args(3))
    }
  }

  def InitData(confPath: String): Unit = {
    val spark = SparkSession.builder.appName("Mysql_2_Orc_Init_No_Partition").enableHiveSupport().getOrCreate

    var confArray: Array[String] = spark.sparkContext.textFile(confPath).collect()
    var conf: String = confArray.mkString("\n")
    val properties = new Properties
    val inputStream = new ByteArrayInputStream(conf.getBytes)
    properties.load(inputStream)
    val threadsNum = properties.getProperty(ETLConst.ETL_THREADS, ETLConst.ETL_THREADS_DEFAUL_VALUE).toInt
    val tablesPath = properties.getProperty(ETLConst.ETL_TABLES_PATH)
    val orcPath = properties.getProperty(ETLConst.ETL_TAGET_PATH)
    val orcCoalesce = properties.getProperty(ETLConst.ETL_COALESCE, ETLConst.ETL_COALESCE_DEFAULT_VALUE).toInt

    val pool = Executors.newFixedThreadPool(threadsNum)
    implicit val xc = ExecutionContext.fromExecutorService(pool)
    val tasks: mutable.MutableList[Future[String]] = mutable.MutableList[Future[String]]()
    var tblsArray: Array[String] = spark.sparkContext.textFile(tablesPath).collect()
    val status: mutable.MutableList[String] = mutable.MutableList[String]()
    for (i <- 0 until tblsArray.length) {
      var sql = "select * from " + tblsArray(i)
      if (dirExists(orcPath + tblsArray(i) + "/_SUCCESS")) {
        println("[WARN]" + orcPath + tblsArray(i) + "/_SUCCESS exist, skipped it")
      } else {
        if (dirExists(orcPath + tblsArray(i))) {
          println("[WARN]" + orcPath + tblsArray(i) + " exist, but " + orcPath + tblsArray(i) + "/_SUCCESS not exist,removing " + orcPath + tblsArray(i) + " and reimport")
          fs.delete(new org.apache.hadoop.fs.Path(orcPath + tblsArray(i)), true)
        }
        val task = doEtlPerTbl(spark, sql, orcPath + tblsArray(i), orcCoalesce)
        task.onComplete {
          case Success(result) =>
            println(s"result = $result")
            status += result
          case Failure(e) =>
            println(e.getMessage)
            status += e.getMessage
        }
        tasks += task
      }

    }

    Await.result(Future.sequence(tasks), Duration(30, DAYS))
    println("[INFO] All Jobs has completed and the informations are:")
    for (i <- 0 until status.length) {
      println("[INFO]" + status(i))
    }
    spark.close()

  }

  def ImportDataDaily(confPath: String, filterCol: String, filterVal1: String, filterVal2: String): Unit = {
    val spark = SparkSession.builder.appName("Mysql_2_ORc_Import_Daily") // optional and will be autogenerated if not specified
      .enableHiveSupport() // self-explanatory, isn't it?
      .getOrCreate

    val confArray: Array[String] = spark.sparkContext.textFile(confPath).collect()
    val conf: String = confArray.mkString("\n")
    val properties = new Properties
    val inputStream = new ByteArrayInputStream(conf.getBytes)
    properties.load(inputStream)
    val threadsNum = properties.getProperty(ETLConst.ETL_THREADS, ETLConst.ETL_THREADS_DEFAUL_VALUE).toInt
    val tablesPath = properties.getProperty(ETLConst.ETL_TABLES_PATH)
    val orcPath = properties.getProperty(ETLConst.ETL_TAGET_PATH)
    val orcCoalesce = properties.getProperty(ETLConst.ETL_COALESCE, ETLConst.ETL_COALESCE_DEFAULT_VALUE).toInt
    println("[DEBUG] "+ETLConst.ETL_COALESCE+" = "+ orcCoalesce)
    val pool = Executors.newFixedThreadPool(threadsNum.toInt)

    implicit val xc = ExecutionContext.fromExecutorService(pool)
    val tasks: mutable.MutableList[Future[String]] = mutable.MutableList[Future[String]]()
    var tblsArray: Array[String] = spark.sparkContext.textFile(tablesPath).collect()
    val status: mutable.MutableList[String] = mutable.MutableList[String]()
    for (i <- 0 until tblsArray.length) {
      var sql = "select * from " + tblsArray(i)
      var partitionPath = "";
      if (filterCol != null && !filterCol.equals("") && filterVal1 != null && !filterVal1.equals("") && filterVal2 != null && !filterVal2.equals("")) {
        val columns: Array[String] = filterCol.split(",")
        val values1: Array[String] = filterVal1.split(",")
        val values2: Array[String] = filterVal2.split(",")
        if (columns.length == values1.length) {
          for (j <- 0 until columns.length) {
            val c_t = columns.apply(j).split(":")
            val col_name=c_t.apply(0)
            val col_type = c_t.apply(1)
            var quotation = "";
            if(!ETLConst.COL_TYPE_NUMBER.contains(col_type.toUpperCase())){
              quotation = "'"
            }

            if (j == 0) {
              sql = sql + " where " + col_name + ">"+quotation + values1.apply(j) + " 00:00:00"+quotation + " and " + col_name + "<"+quotation + values2.apply(j) + " 00:00:00"+quotation
            } else {
              sql = sql + " and " + col_name + ">"+quotation + values1.apply(j) + " 00:00:00"+quotation + " and " + col_name + "<"+quotation + values2.apply(j) + " 00:00:00"+quotation
            }
            partitionPath = partitionPath + "/" + col_name + "=" + values1.apply(j)
          }
        } else {
          println("[ERROR] columns length is not equal with values length")
        }
      }
      if (dirExists(orcPath + tblsArray(i) + partitionPath + "/_SUCCESS")) {
        println("[WARN]" + orcPath + tblsArray(i) + partitionPath + "/_SUCCESS exist, skipped it")
      } else {
        if (dirExists(orcPath + tblsArray(i) + partitionPath)) {
          println("[WARN]" + orcPath + tblsArray(i) + partitionPath + " exist, but " + orcPath + tblsArray(i) + partitionPath + "/_SUCCESS not exist,removing " + orcPath + tblsArray(i) + partitionPath + " and reimport")
          fs.delete(new org.apache.hadoop.fs.Path(orcPath + tblsArray(i) + partitionPath), true)
        }
        val task = doEtlPerTbl(spark, sql, orcPath + tblsArray(i) + partitionPath, orcCoalesce)
        task.onComplete {
          case Success(result) =>
            println(s"result = $result")
            status += result
          case Failure(e) =>
            println(e.getMessage)
            status += e.getMessage
        }
        tasks += task
      }

    }

    Await.result(Future.sequence(tasks), Duration(30, DAYS))
    println("[INFO] All Jobs has completed and the informations are:")
    for (i <- 0 until status.length) {
      println("[INFO]" + status(i))
    }
    spark.close()
  }

  def castInt2BigInt(df:DataFrame,columnIndex:scala.Int): DataFrame ={
    df.cache()
    df.schema(df.columns.apply(columnIndex)).dataType match {
      case IntegerType =>
        ColumnUtil.castColumnTo(df,df.columns.apply(columnIndex),LongType)
      case _ => df
    }

  }

  def castInt2BigInt(df:DataFrame): DataFrame ={
    var finalDf = df
    for (i <- 0 until  df.columns.length) {
      finalDf = castInt2BigInt(finalDf,i)
    }
    return finalDf
  }

  def doEtlPerTbl(ss: SparkSession, sql: String, outPath: String, orcCoalesce: Int)(implicit xc: ExecutionContext) = Future {
    var message = "[INFO] execute the sql : " + sql + " and write the orc file in " + outPath + " with orcCoalesce: "+orcCoalesce+" successfully!"
    println(message)
    val df = castInt2BigInt(ss.sql(sql))
    println("[INFO] The final schema infomation is:")
    df.printSchema()
    if(0 == orcCoalesce){
      df.write.orc(outPath)
    }else {
      df.coalesce(orcCoalesce).write.orc(outPath)
    }
    message
  }

  def dirExists(hdfsDirectory: String): Boolean = {
    val exists = fs.exists(new org.apache.hadoop.fs.Path(hdfsDirectory))
    return exists
  }
}
