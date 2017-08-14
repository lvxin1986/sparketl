package com.jd.etl

import java.io.ByteArrayInputStream
import java.util.Properties
import java.util.concurrent.Executors

import org.apache.spark.sql.SparkSession

import scala.collection.mutable
import scala.concurrent._
import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Created by root on 17-8-1.
  */
object Mysql2OrcInit {
    def main(args: Array[String]) {
        var confPath = args(0)

        val spark = SparkSession.builder.appName("Mysql2ORcInit") // optional and will be autogenerated if not specified
//            .master("yarn")               // avoid hardcoding the deployment environment
            .enableHiveSupport()              // self-explanatory, isn't it?
            //      .config("spark.sql.warehouse.dir", "target/spark-warehouse")
//            .config("spark.shuffle.service.enabled","true")
//            .config("spark.dynamicAllocation.enabled","true")
//            .config("spark.executor.cores","5")
          .getOrCreate
        spark.conf.set("spark.sql.shuffle.partitions", 1)
//        spark.conf.set("spark.executor.memory", "2g")

        var confArray : Array[String] = spark.sparkContext.textFile(confPath).collect()
        var conf : String = confArray.mkString("\n")
        val properties = new Properties
        val inputStream = new ByteArrayInputStream(conf.getBytes)
        properties.load(inputStream)
        var threadsNum = properties.getProperty("etl.mysql2orc.threads.num")
        var tablesPath = properties.getProperty("etl.mysql2orc.tables.path")
        //var orcPath= args(0)
        var orcPath = properties.getProperty("etl.mysql2orc.orc.path")
        // Set number of threads via a configuration property
        val pool = Executors.newFixedThreadPool(threadsNum.toInt)

        // create the implicit ExecutionContext based on our thread pool
        implicit val xc = ExecutionContext.fromExecutorService(pool)
        val tasks: mutable.MutableList[Future[String]] = mutable.MutableList[Future[String]]()
        var tblsArray : Array[String] = spark.sparkContext.textFile(tablesPath).collect()
        val status: mutable.MutableList[String] = mutable.MutableList[String]()
        //    for(i <- 0 until tblsArray.length){
        //      spark.sql("select * from "+tblsArray(i)).write.orc(orcPath+tblsArray(i)+".orc")
        //    }
        for(i <- 0 until tblsArray.length){
            //      spark.sql("select * from "+tblsArray(i)).write.orc(orcPath+tblsArray(i)+".orc")
            var sql = "select * from " + tblsArray(i)
            val task = doEtlPerTbl(spark, sql, orcPath+tblsArray(i))
            task.onComplete {
                case Success(result) =>
                    println(s"result = $result")
                    status+=result
                case Failure(e) =>
                    println(e.getMessage)
                    status+=e.getMessage
            }
            tasks+=task
        }
        //    while (status.size < tasks.size)
        //    {
        //      println("[INFO] Total Jobs number is:" + tasks.size +" and completed: "+status.size )
        //      Thread.sleep(1000)
        //    }


        // Now wait for the tasks to finish before exiting the app
        Await.result(Future.sequence(tasks), Duration(1, DAYS))
        println("[INFO] All Jobs has completed and the informations are:" )
        for(i <- 0 until status.length){
            println("[INFO]" +status(i))
        }
        //    spark.sparkContext.stop()
        spark.close()
        //    Runtime.getRuntime.exit(0);

    }

    def doEtlPerTbl(ss: SparkSession, sql:String,outPath: String)(implicit xc: ExecutionContext) = Future {
        var message = "NULl";
        ss.sql(sql).coalesce(1).write.orc(outPath)
        message = "[INFO] execute the sql : "+sql+" and write the orc file in "+outPath+" successfully!"
        message

    }

}
