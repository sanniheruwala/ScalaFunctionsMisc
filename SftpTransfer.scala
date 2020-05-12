package com.rxcorp.bdf.datalake.de9.sftp

import java.util.Properties

import com.jcraft.jsch.{Channel, ChannelSftp, JSch, Session}
import org.apache.hadoop.fs._

/**
  * Created by SHeruwala on 6/10/2018.
  */
object SftpTransfer {

  /**
    * SFTP connection channel context.
    *
    * @param host host url
    * @param port default port / 22
    * @param user sftp username
    * @param pass sftp password
    * @return Channel context
    */
  def sftpConnect(host: String, port: Int, user: String, pass: String): ChannelSftp = {
    val jsch: JSch = new JSch()
    val session: Session = jsch.getSession(user, host, port)
    val config: Properties = new Properties()
    config.put("StrictHostKeyChecking", "no")
    session.setConfig(config)
    session.setPassword(pass)
    session.connect()
    val channel: Channel = session.openChannel("sftp")
    channel.connect()
    val sftpChanel: ChannelSftp = channel.asInstanceOf[ChannelSftp]
    sftpChanel
  }

  /**
    * Connect to sftp location using JSCH lib.
    *
    * @param host              host url
    * @param port              default port / 22
    * @param user              sftp user name
    * @param pass              sftp password
    * @param srcFolder         hdfs dir
    * @param DestinationFolder sftp dir
    * @param fs                hadoop fs context
    * @return Nothing / Just upload
    */
  def uploadFiles(host: String, port: Int, user: String, pass: String, srcFolder: String, DestinationFolder: String, fs: FileSystem): Int = {
    val trgtLoc = s"$DestinationFolder"
    val sftpChanel: ChannelSftp = sftpConnect(host, port, user, pass)
    copyFromHdfsFiles(srcFolder, fs, trgtLoc, sftpChanel)
    sftpChanel.disconnect()
    0
  }

  /**
    * Pass the jsch connection object and upload
    * files from HDFS to SFTP location.
    *
    * Dev BenchMark :- 70GB in 134 min
    * Prod BenchMark :- 35GB in 20 min
    *
    * @param srcFolder hdfs location
    * @param fs        hadoop fs context
    * @param trgtLoc   sftp target dir
    * @param client    conection client
    */
  def copyFromHdfsFiles(srcFolder: String, fs: FileSystem, trgtLoc: String, client: ChannelSftp): Int = {
    val listofFiles: Array[FileStatus] = fs.listStatus(new Path(srcFolder))
    println(listofFiles)
    for (files <- listofFiles) {
      if (fs.isDirectory(files.getPath)) {
        val directoryPath: String = files.getPath.toString.substring(files.getPath.toString.lastIndexOf("/") + 1)
        println(directoryPath)
        val dirstatus: Unit = client.mkdir(s"$trgtLoc")
        copyFromHdfsFiles(files.getPath.toString, fs, s"$trgtLoc", client)
      } else {
        val hdfspath: String = files.getPath.toString
        val filename: String = hdfspath.substring(hdfspath.lastIndexOf("/") + 1)
        val remoteFile: String = s"$trgtLoc/$filename"
        println(remoteFile)
        val inputStream: FSDataInputStream = fs.open(files.getPath())
        client.put(inputStream, remoteFile)
      }
    }
    0
  }

  /**
    * Connect to sftp location using JSCH lib.
    *
    * @param host              host url
    * @param port              default port / 22
    * @param user              sftp user name
    * @param pass              sftp password
    * @param srcFolder         SFTP dir
    * @param DestinationFolder hdfs dir
    * @param fs                hadoop fs context
    * @return Nothing / Just download
    */
  def downloadFiles(host: String, port: Int, user: String, pass: String, srcFolder: String, DestinationFolder: String, fs: FileSystem): Int = {
    val trgtLoc = s"$DestinationFolder"
    val sftpChanel: ChannelSftp = sftpConnect(host, port, user, pass)
    copyToHdfsFiles(host, port, user, pass, srcFolder, trgtLoc, sftpChanel, fs)
    sftpChanel.disconnect()
    0
  }

  def deleteFiles(host: String, port: Int, user: String, pass: String, destinationFile: String): Int = {
    val sftpChanel: ChannelSftp = sftpConnect(host, port, user, pass)
    sftpChanel.rm(destinationFile)
    sftpChanel.disconnect()
    0
  }

  /**
    * Download file to hdfs dir
    * from sftp location.
    *
    * @param srcFile SFTP location
    * @param fs      hadoop fs context
    * @param trgtLoc hdfs dir
    * @param client  connection client
    */
  def copyToHdfsFiles(host: String, port: Int, user: String, pass: String, srcFile: String, trgtLoc: String, client: ChannelSftp, fs: FileSystem): Int = {
    println("Clearrr##################" + srcFile + "######################")
    val outputStream: FSDataOutputStream = fs.create(new Path(trgtLoc), true)
    client.get(srcFile, outputStream)
    outputStream.close()
    client.exit()
    0
  }

  /**
    * List all files at sftp location.
    *
    * @param host   host url
    * @param port   default port / 22
    * @param user   sftp user name
    * @param pass   sftp password
    * @param srcDir SFTP dir
    * @return List of files at location
    */
  def listFiles(host: String, port: Int, user: String, pass: String, srcDir: String): Seq[String] = {
    val sftpChanel: ChannelSftp = sftpConnect(host, port, user, pass)
    val lst = sftpChanel.ls(srcDir).toArray.map(x => x.toString.split(" ").last).toList
    sftpChanel.disconnect()
    lst
  }

  /**
    * Move file in sftp from one dir
    * to another dir.
    *
    * @param host     host url
    * @param port     default port / 22
    * @param user     sftp user name
    * @param pass     sftp password
    * @param srcFile  SFTP dir
    * @param TrgtFile SFTP target file
    */
  def moveFile(host: String, port: Int, user: String, pass: String, srcFile: String, TrgtFile: String): Int = {
    val sftpChanel: ChannelSftp = sftpConnect(host, port, user, pass)
    sftpChanel.rename(srcFile, TrgtFile)
    sftpChanel.disconnect()
    0
  }
}
