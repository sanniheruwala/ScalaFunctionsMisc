package com.rxcorp.bdf.datalake.de9.validation

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.postfixOps

object QcFun {

  def nullValidations(spark: SparkSession, tableName: String, validationTable: String, db: String, colNull: List[String]): (Boolean, List[String]) = {
    val de9_tbl: DataFrame = spark.read.table(db + "_batch" + "." + validationTable)
    val nullres: List[String] = colNull.filter { (x: String) => de9_tbl.filter(de9_tbl(x).isNull).count() != 0 }
    var nullCheck = (true, nullres)
    if (nullres.isEmpty == false) {
      nullCheck = (false, nullres)
    }

    nullCheck
  }

  def cntValidation(spark: SparkSession, tableName: String, batchTable: String, db: String): Boolean = {
    val de9_tbl: DataFrame = spark.read.table(db + "_batch" + "." + batchTable)
    var cntCheck = true
    if (de9_tbl.count() == 0) {
      cntCheck = false
    }
    cntCheck
  }

  def cntMtchValidation(spark: SparkSession, tableName: String, batchTable: String, db: String, hdfsPath: String, header: String): Boolean = {
    var cntMatch = true
    val de9_tbl: DataFrame = spark.read.table(db + "_batch" + "." + batchTable)
    if (hdfsPath.contains(".parquet") == false) {
      val text_file = spark.sparkContext.textFile(hdfsPath)
      val firstRow = text_file.first
      val fileFinal = text_file.filter(x => x != firstRow)
      if (de9_tbl.count() != text_file.count() && header == "N") {
        cntMatch = false
      } else if (de9_tbl.count() != fileFinal.count() && header == "Y" && hdfsPath.contains(".parquet") == false) {
        cntMatch = false
      }
    } else {
      if (hdfsPath.contains(".parquet") == true) {
        val countCheck = spark.read.load(hdfsPath).count
        if (countCheck != de9_tbl.count()) cntMatch = false
      }
    }
    cntMatch
  }

  def dupValidation(spark: SparkSession, tableName: String, validationTable: String, db: String, colDup: Seq[String]): (Boolean, Long) = {
    val de9_tbl: DataFrame = spark.read.table(db + "_batch" + "." + validationTable)

    def mMatch(colDup: Seq[String]) = {
      colDup match {
        case null => de9_tbl
        case Seq() => de9_tbl
        case _ => de9_tbl.selectExpr(colDup: _*)
      }
    }

    val cnt = mMatch(colDup).count()
    val dist = mMatch(colDup).distinct().count()
    var duplicateCheck: (Boolean, Long) = (true, 0)
    if (cnt != dist) duplicateCheck = (false, cnt - dist)
    duplicateCheck
  }

  def customValidation(spark: SparkSession, cusQueries: String, ac: String): (Boolean, String) = {
    var cusQueryCheck: (Boolean, String) = (true, "No custom check")
    if (cusQueries != null && !cusQueries.isEmpty) {
      val out = spark.sql(cusQueries)
      import spark.implicits._
      val accDf = List(ac).toDF()
      val res = accDf.intersect(out).count().toString
      println(res)
      if (res == "1") {
        cusQueryCheck = (true, ac)
      } else {
        cusQueryCheck = (false, "")
      }
    }
    cusQueryCheck
  }

  def cntMatchExasol(spark: SparkSession, tableName: String, batchTable: String, db: String, exasolView: String, viewType: String, periodFm: String, periodTo: String): Boolean = {
    var cntMatch = true
    val de9_tbl: DataFrame = spark.read.table(db + "_batch" + "." + batchTable)
    val jdbcProps = new Properties()
    jdbcProps.put("user", "CH_FEED")
    jdbcProps.put("password", "HuKg26LmG+1")
    jdbcProps.put("driver", "com.exasol.jdbc.EXADriver")
    var sqlQry = s"(Select count(*) as count from DM_PROD_CH_FEED.$exasolView)"

    if (viewType == "M" && periodFm != "0" && periodTo != "0") {
      sqlQry = s"(Select count(*) as count from DM_PROD_CH_FEED.$exasolView where mth_id between $periodFm and $periodTo)"
    } else {
      if (viewType == "W" && periodFm != "0" && periodTo != "0") {
        sqlQry = s"(Select count(*) as count from DM_PROD_CH_FEED.$exasolView where wk_id between $periodFm and $periodTo)"
      }
    }

    val resultDF: DataFrame = spark.read.jdbc("jdbc:exa:162.44.26.71..80:8563", sqlQry, jdbcProps)

    import spark.implicits._
    val acc = List(de9_tbl.count()).toDF("count")

    val res = acc.intersect(resultDF).count().toString
    println(res)
    if (res != "1") {
      cntMatch = false
    }
    cntMatch
  }
}
