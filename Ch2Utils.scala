package com.rxcorp.bdf.datalake.de9.utils

import com.rxcorp.bdf.datalake.de9.sftp.FtpConfig
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory, CompressionInputStream}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.postfixOps

/**
  * Created by SHeruwala on 7/21/2018.
  */
object Ch2Utils extends FtpConfig {

  /**
    * Load external table with csv file as base line.
    * @param tableName TableName
    * @param spark SparkSession
    * @param db Database env based
    * @param delimiter delimiter from file
    */
  def loadExternal(tableName: String,spark: SparkSession,db: String,delimiter: String): Unit = {

    val dir = spark.sql(s"""show create table ${db}_batch.${tableName}_load""")
      .head
      .getString(0)
      .split(" ")
      .filter(x=>x.contains(s"${tableName}_load") && x.contains("hdfs"))(0)
      .split("\n")(0)
      .replaceAll("'","")

    val data =spark.read
      .format("org.apache.spark.csv")
      .option("header", true)
      .option("inferSchema", true)
      .option("delimiter",delimiter)
      .csv(dir)
      .createTempView(s"${tableName}_load")

  }

  /**
    * A/B switch between inactive and
    * active tables.
    *
    * @param db        Database Name
    * @param tableName Tables to switch
    * @param spark     SparkSession context
    * @return Success
    */
  def switchTables(db: String, tableName: String, spark: SparkSession): Int = {
    spark.sql(s"alter table  ${db}.${tableName} rename to ${db}.${tableName}_TEMP")
    spark.sql(s"msck repair table ${db}.${tableName}_TEMP")
    spark.sql(s"alter table  ${db}.${tableName}_INA rename to ${db}.${tableName}")
    spark.sql(s"msck repair table ${db}.${tableName}")
    spark.sql(s"alter table  ${db}.${tableName}_TEMP rename to ${db}.${tableName}_INA")
    spark.sql(s"msck repair table ${db}.${tableName}_ina")

    0
  }

  def switchTableswoutPartition(db: String, tableName: String, spark: SparkSession): Int = {
    spark.sql(s"alter table  ${db}.${tableName} rename to ${db}.${tableName}_TEMP")
    spark.sql(s"refresh table ${db}.${tableName}_TEMP")
    spark.sql(s"alter table  ${db}.${tableName}_INA rename to ${db}.${tableName}")
    spark.sql(s"refresh table ${db}.${tableName}")
    spark.sql(s"alter table  ${db}.${tableName}_TEMP rename to ${db}.${tableName}_INA")
    spark.sql(s"refresh table ${db}.${tableName}_INA")
    0
  }

  /**
    * Maintain object refresh and its timing.
    *
    * @param spark      Spark context
    * @param table_name table name
    * @param start_time start time
    * @param end_time   end time
    * @param status     success/failure
    * @param count      record count
    * @param db         database name
    * @return success
    */
  def auditTables(spark: SparkSession, table_name: String, start_time: java.sql.Timestamp, end_time: java.sql.Timestamp, status: String, count: Int, db: String): Int = {

    val data = (table_name, db, start_time, end_time, status, count)
    var auditList = List[(String, String, java.sql.Timestamp, java.sql.Timestamp, String, Int)]()
    auditList = auditList.:+(data)

    import spark.implicits._
    val audit: DataFrame = auditList.toDF("table_name", "db_name", "start_time", "end_time", "status", "count")

    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    audit.write.format("parquet").insertInto(s"${db}.de9_ch2_object_audit")
    0
  }

  /**
    * For list of week_id(partition key) check which are
    * not in synce and return list of week_ids.
    *
    * @param srcTable  Source table name i.e. de9_kf_pharmacy_fact
    * @param trgtTable Target table name i.e. de9_kf_pharmacy_fact_ina
    * @param db        database name
    * @param spark     SparkSession context
    */
  def listUnsyncedWeekId(srcTable: String, trgtTable: String, db: String, spark: SparkSession): Seq[String] = {

    val src: DataFrame = spark.sql(s"select count(*) as count,week_id from ${db}.${srcTable} group by week_id")
    val trgt: DataFrame = spark.sql(s"select count(*) as count,week_id from ${db}.${trgtTable} group by week_id")

    src.except(trgt).select("week_id").collect().toList.map(_.toString())
  }

  /**
    * For sync of INA and actual table during
    * incremental load used.
    *
    * @param srcWh  Source table path
    * @param trgtWh Target table path
    * @return Success/Failure
    */
  def syncData(srcWh: String, trgtWh: String): Int = {
    import sys.process._

    val queue: String = "de9"
    s"hadoop distcp -Dmapred.job.queue.name=$queue -update -skipcrccheck  $srcWh $trgtWh" !
  }

  /**
    * For sync of INA and actual table during load
    * is being used which is having partitioned table.
    *
    * @param srcWh          Source table path
    * @param trgtWh         Target table path
    * @param partitionKey   Partition column value
    * @param partitionValue Partition column value
    * @return Success/Failure
    */
  def syncDataPartition(srcWh: String, trgtWh: String, partitionKey: String, partitionValue: String): Int = {
    import sys.process._

    val queue: String = "de9"
    s"hadoop distcp -Dmapred.job.queue.name=$queue -update -skipcrccheck  $srcWh/$partitionKey=$partitionValue $trgtWh/$partitionKey=$partitionValue" !
  }

  /**
    * unzip files inside the HDFS
    * and delete the zip file.
    *
    * @param extPath HDFS dir path
    * @param status  To maintain Pipeline
    * @return count of records from file
    */
  def unzipFile(extPath: String, status: String, spark: SparkSession, fs: FileSystem): Long = {

    /**
      * Compress Factory to detect compression codec
      * and create input stream by reading from .gz file
      * and output it to same hdfs dir.
      */
    val factory: CompressionCodecFactory = new CompressionCodecFactory(conf)
    val codec: CompressionCodec = factory.getCodec(new Path(extPath))

    var in: Null = null
    var out: Null = null

    /**
      * Delete all .gz file after extraction
      * and return the count of file by reading through
      * spark dataframe.
      */
    try {
      var in: CompressionInputStream = codec.createInputStream(fs.open(new Path(extPath)))
      var out: FSDataOutputStream = fs.create(new Path(extPath.replaceAll(".gz", "")))
      IOUtils.copyBytes(in, out, conf)
      fs.delete(new Path(extPath), true)
      spark.read.textFile(extPath.replaceAll(".gz", "")).count()
    } catch {
      case ex: Exception => ex.printStackTrace()
        0
    } finally {
      /**
        * Close both input
        * and output streams.
        */
      IOUtils.closeStream(in)
      IOUtils.closeStream(out)
    }
  }
}
