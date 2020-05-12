package com.rxcorp.bdf.datalake.de9.sftp

import com.rxcorp.bdf.datalake.de9.utils.Logger.logger
import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream, FileSystem, Path}
import org.apache.hadoop.io.IOUtils
import org.apache.spark.sql.SparkSession

/**
  * Created by SHeruwala on 7/19/2018.
  */
object FtpDownload extends FtpConfig {

  /**
    * Download the file from the given
    * source file path as input.
    * Puts FTP data directly to HDFS dir.
    *
    * @param fileName source file name
    * @return Returns process status
    */
  def download(srcDir: String, extTablePath: String, fileName: String, spark: SparkSession): (String, Long) = {
    val srcFile: String = srcDir + fileName
    try {
      logger.info("Ftp Download Process Started for " + srcFile)
      val fsdin: FSDataInputStream = ftpfs.open(new Path(srcFile), 1000)
      val fileSystem: FileSystem = FileSystem.get(conf)
      val outputStream: FSDataOutputStream = fileSystem.create(new Path(extTablePath + fileName))
      IOUtils.copyBytes(fsdin, outputStream, conf, true)
      logger.info("Ftp Download Process Finished for " + srcFile)
      val count: Long = spark.read.textFile(extTablePath + fileName).count()
      ("Success", count)
    }
    catch {
      case ex: Exception => ex.printStackTrace()
        ("Failed", 0)
    }
  }
}
