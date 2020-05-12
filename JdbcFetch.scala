package com.rxcorp.bdf.datalake.de9.jdbc

import java.io.{File, FileOutputStream}
import java.text.SimpleDateFormat
import java.util.{Calendar, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{SaveMode, SparkSession}

object JdbcFetch {

  def main(args: Array[String]): Unit = {
    val engine = args(0)
    val jdbcUrl = args(1)
    val jdbcUsr = args(2)
    val jdbcPass = args(3)
    val fetchOption = args(4)
    val fetchCommand = args(5)
    val trgtOption = args(6)
    val trgtLocation = args(7)

    val jdbcRead = new JdbcFetch()

    jdbcRead.readFromJDBC(engine, jdbcUrl, jdbcUsr, jdbcPass, fetchOption, fetchCommand, trgtOption, trgtLocation);

  }

}

class JdbcFetch {

  def readFromJDBC(eng: String, jdbcURL: String, srcUsr: String, srcPass: String, fetchOpt: String, fetchCmd: String, trgtOpt: String, trgtLoc: String): Unit = {

    val config = ConfigFactory.load()
    val dateFormat = new SimpleDateFormat("ddMMyyyy")
    val queryDate = dateFormat.format(Calendar.getInstance().getTime)
    var sqlQry = ""

    val conf = new Configuration
    conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"))
    conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"))
    val fs = FileSystem.get(conf)


    val sparkSess = SparkSession.builder().appName("JDBCRead").enableHiveSupport().getOrCreate()

    sparkSess.sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")


    val jdbcProps = new Properties()
    jdbcProps.put("user", srcUsr)
    jdbcProps.put("password", srcPass)
    jdbcProps.put("driver", eng)

    if (fetchOpt == "table") {
      sqlQry = s"(Select * from $fetchCmd) asd"
    }
    else if (fetchOpt == "sql") {
      sqlQry = s"(${fetchCmd}) asd"
    }

    try {

      val resultDF = sparkSess.read.jdbc(jdbcURL, sqlQry, jdbcProps)


      if (trgtOpt == "file") {
        resultDF.coalesce(1).write.mode(SaveMode.Overwrite).parquet(s"$trgtLoc")
      }
      else if (trgtOpt == "table") {
        resultDF.write.mode(SaveMode.Overwrite).saveAsTable(s"$trgtLoc")
      }
      exit(0)
    }

    catch {
      case ex: Exception => {
        println(ex.getMessage)
        exit(1)
      }
    }
  }

  def exit(status: Int): Unit = {
    val props = new Properties()
    val propPath = System.getProperty("oozie.action.output.properties")
    if (propPath != null) {
      val file = new File(propPath);
      props.setProperty("exitScript", "" + status);

      val os = new FileOutputStream(file);
      props.store(os, "");
      os.close();
    }

    System.out.println("exitScript=" + status);
    if (status == 1) {
      System.exit(1);
    }
  }

}
