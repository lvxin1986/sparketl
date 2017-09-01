package com.jd.etl.utils

import com.jd.etl.consts.ETLConst
import org.apache.hadoop.fs.FileSystem
import org.apache.spark.sql._
import org.apache.spark.sql.types.{LongType, IntegerType}

/**
  * Created by ninet on 17-8-21.
  */
package object EtlUtil {
    def castInt2BigInt(df: DataFrame, columnIndex: scala.Int): DataFrame = {

        df.schema(df.columns.apply(columnIndex)).dataType match {
            case IntegerType =>
                ColumnUtil.castColumnTo(df, df.columns.apply(columnIndex), LongType)
            case _ => df
        }

    }

    def castInt2BigInt(df: DataFrame): DataFrame = {
        var finalDf = df
        for (i <- 0 until df.columns.length) {
            finalDf = castInt2BigInt(finalDf, i)
        }
        return finalDf
    }

    def generateSql(schema_info: Map[String, String], tbl_schema_pair: Array[String], filterCol: String, filterVal1: String, filterVal2: String): String = {
        var sql: String = ""
        if (schema_info.equals("") || schema_info == null) {
            sql = "select * from " + tbl_schema_pair.apply(0)
        } else {
            sql = "select " + schema_info.apply(tbl_schema_pair.apply(1)) + " from " + tbl_schema_pair.apply(0)
        }
        val partitionCols: Array[String] = filterCol.split(",")
        val values1: Array[String] = filterVal1.split(",")
        val values2: Array[String] = filterVal2.split(",")
        if (partitionCols.length == values1.length) {
            for (j <- 0 until partitionCols.length) {
                val partition_col_name_pair = partitionCols.apply(j).split(":")
                val partition_col_name = partition_col_name_pair.apply(0)
                val partition_col_type = partition_col_name_pair.apply(1)
                var quotation = ""
                if (!ETLConst.COL_TYPE_NUMBER.contains(partition_col_type.toUpperCase())) {
                    quotation = "'"
                }

                if (j == 0) {
                    sql = sql + " where " + partition_col_name + ">=" + quotation + values1.apply(j) + " 00:00:00" + quotation + " and " + partition_col_name + "<" + quotation + values2.apply(j) + " 00:00:00" + quotation
                } else {
                    sql = sql + " or " + partition_col_name + ">=" + quotation + values1.apply(j) + " 00:00:00" + quotation + " and " + partition_col_name + "<" + quotation + values2.apply(j) + " 00:00:00" + quotation
                }
            }
        }
        return sql
    }

    def dirExists(fs: FileSystem, hdfsDirectory: String): Boolean = {
        val exists = fs.exists(new org.apache.hadoop.fs.Path(hdfsDirectory))
        return exists
    }

    def initSchemaMap(schmsArray: Array[String]): Map[String, String] = {
        var schemaMap: Map[String, String] = Map()
        for (i <- 0 until schmsArray.length) {
            var schema: String = schmsArray.apply(i)
            var strs: Array[String] = schema.split("\\s+")
            var key: String = strs.apply(0)
            var value: String = strs.apply(1)
            value = value.subSequence(7, value.length - 1).toString
            var colAndVals: Array[String] = value.split(",")
            var columns: String = ""
            for (j <- 0 until colAndVals.length) {
                var colAndVal = colAndVals.apply(j).split(":")
                var column: String = colAndVal.apply(0)
                if (j == 0) {
                    columns = column
                } else {
                    columns = columns + "," + column
                }
            }
            println("[INITMAP] " + key + " = " + columns)
            schemaMap += (key -> columns)
        }
        return schemaMap
    }
}
