package com.rxcorp.bdf.datalake.de9.trxAsset.cdc

import com.rxcorp.bdf.datalake.de9.sftp.FtpConfig
import org.apache.hadoop.fs.{FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.compress.{CompressionCodec, CompressionCodecFactory, CompressionInputStream}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.language.postfixOps

/**
  * Created by SHeruwala on 7/21/2018.
  */
object CdcUtils extends FtpConfig {

  /**
    * For list of week_id(partition key) check which are
    * not in synce and return list of week_ids.
    *
    * @param srcTable  Source table name i.e. de9_kf_pharmacy_fact
    * @param trgtTable Target table name i.e. de9_kf_pharmacy_fact_ina
    * @param db        database name
    * @param spark     SparkSession context
    */
  def listUnsyncedWeekId(srcTable: String, trgtTable: String, db: String, spark: SparkSession, col: String): Seq[String] = {

    val src: DataFrame = spark.sql(s"select count(*) as count,$col from ${db}.${srcTable} group by $col")
    val trgt: DataFrame = spark.sql(s"select count(*) as count,$col from ${db}.${trgtTable} group by $col")

    src.except(trgt).select(col).distinct().collect().toList.map(_.getInt(0).toString)
  }

  /**
    * For sync of INA and actual table during load
    * is being used.
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
    * is being used.
    *
    * @param srcWh  Source table path
    * @param trgtWh Target table path
    * @return Success/Failure
    */
  def syncDataPartition(srcWh: String, trgtWh: String, colName: String, value: String): Int = {
    import sys.process._

    val queue: String = "de9"
    s"hadoop distcp -Dmapred.job.queue.name=$queue -update -skipcrccheck  $srcWh/$colName=$value $trgtWh/$colName=$value" !
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
