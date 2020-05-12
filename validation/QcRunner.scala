package com.rxcorp.bdf.datalake.de9.validation

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import scala.language.postfixOps

object QcRunner {
  var body = "Hi Team, \n\nBelow is the result from QC process: \n"
  var mails = ""
  var isMailTrigger = "Y"
  var targetTable = ""
  var alertMsg = Map("F" -> "Failure", "W" -> "Warning")
  var logPath = ""

  def runValidation(spark: SparkSession, parms: List[String], db: String,fs: FileSystem): Int = {

    try {
      println(parms.mkString(","))
      var isValid = true
      if(parms.length >= 25) {
        targetTable = parms(1)
        val batchTable = parms(2)
        val validationTable = parms(3)
        val isHeader = parms(4)
        val countCheck = parms(5)
        val countErrorType = parms(6)
        val countMatchCheck = parms(7)
        val HdfsDirPath = parms(8)

        if (countCheck == "Y") {
          println("Count check running ... ")
          //count check
          try {

            val cntOutput = QcFun.cntValidation(spark, targetTable, batchTable, db)
            if (cntOutput == false && isValid == true && countErrorType == "F") {
              isValid = false
            }

            body = body + countCheckEmailBodyBuilder(cntOutput, countErrorType)
          } catch {
            case e: Exception => body = body + countCheckEmailBodyBuilderExcp(e.getMessage().toString())
              isValid = false
          }

          //count match check
          try {
            val cntMatchOutput = QcFun.cntMtchValidation(spark, targetTable, batchTable, db, HdfsDirPath, isHeader)
            if (cntMatchOutput == false && isValid == true && countErrorType == "F") {
              isValid = false
            }
            body = body + countMatchCheckEmailBodyBuilder(cntMatchOutput, countErrorType)
          } catch {
            case e: Exception => body = body + countMatchCheckEmailBodyBuilderExcp(e.getMessage().toString())
              isValid = false
          }
          println("Count : " + body)
        }
        //null check
        val nullCheck = parms(9)
        val nullErrorType = parms(10)
        val nullColumns = parms(11).split(",").toList

        if (nullCheck == "Y") {
          println("Null check running ... ")
          try {
            val nullOutput = QcFun.nullValidations(spark, targetTable, validationTable, db, nullColumns)

            if (nullOutput._1 == false && isValid == true && nullErrorType == "F") {
              isValid = false
            }
            body = body + nullCheckEmailBodyBuilder(nullOutput._1, nullOutput._2, nullColumns.mkString(",").toString, nullErrorType)
          } catch {
            case e: Exception => body = body + nullCheckEmailBodyBuilderExcp(nullColumns.mkString(",").toString, e.getMessage().toString())
              isValid = false
          }
          println("Null Check : " + body)
        }

        //duplicate check
        val duplicateCheck = parms(12)
        val duplicateErrorType = parms(13)
        val duplicateColumns = parms(14).split(",").toSeq

        if (duplicateCheck == "Y") {
          println("Duplicate check running ... ")
          try {
            val duplicateOutput = QcFun.dupValidation(spark, targetTable, validationTable, db, duplicateColumns)
            if (duplicateOutput._1 == false && isValid == true && duplicateErrorType == "F") {
              isValid = false
            }
            body = body + duplicateCheckEmailBodyBuilder(duplicateOutput._1, duplicateOutput._2, duplicateColumns.mkString(",").toString, duplicateErrorType)
          } catch {
            case e: Exception => body = body + duplicateCheckEmailBodyBuilderExcp(duplicateColumns.mkString(",").toString, e.getMessage().toString())
              isValid = false
          }
          println("Duplicate : " + body)
        }

        //count match with exasol
        val exasolCheck = parms(15)
        val exasolErrorType = parms(16)
        val exasolViewName = parms(17)
        val exasolPeriodFrm = parms(18)
        val exasolPeriodTo = parms(19)
        val exasolLoadType = parms(20)
        if (exasolCheck == "Y") {
          println("Exasol count match check")
          try {
            val cntExaMatchOutput = QcFun.cntMatchExasol(spark, targetTable, batchTable, db, exasolViewName, exasolLoadType, exasolPeriodFrm, exasolPeriodTo)
            if (cntExaMatchOutput == false && isValid == true && exasolErrorType == "F") {
              isValid = false
            }
            body = body + exasolMatchCheckEmailBodyBuilder(cntExaMatchOutput, exasolErrorType)
          }
          catch {
            case e: Exception => body = body + exasolMatchCheckEmailBodyBuilderExcp(e.getMessage().toString())
              isValid = false
          }
          println("Exasol count match check : " + body)
        }


        isMailTrigger = parms(21)
        mails = parms(22)
        logPath = parms(23)

        val numCustomCheck = parms(24).toInt
        var index = 24

        //custom query check
        for (i <- 1 to numCustomCheck) {
          index += 1
          val customErrorType = parms(index)
          index += 1
          val customQuery = parms(index).replaceAll("#", " ")
          index += 1
          val customExpectedValue = parms(index)
          try {
            println("Query :" + customQuery)
            val cusOutput: (Boolean, String) = QcFun.customValidation(spark, customQuery, customExpectedValue)
            if (cusOutput._1 == false && isValid == true && customErrorType == "F") {
              isValid = false
            }
            body = body + customQueryEmailBodyBuilder(customQuery, customExpectedValue, cusOutput._1, cusOutput._2, customErrorType)
          } catch {
            case e: Exception => body = body + customQueryEmailBodyBuilderExcp(customQuery, customExpectedValue, e.getMessage().toString())
              isValid = false
          }
        }
        println("Custom Check : " + body)
      }
      else
      {
          isValid=false
          body=body + "\n Not enough argument has been passed \n"
      }
      if (isValid == false) {
        1
      } else {
        0
      }
    } catch {
      case e: Exception => //logger.error("QC Runner failed with error : " + e.printStackTrace().toString)
        e.printStackTrace().toString()
        body = body + exceptionBody(e.getMessage().toString())
        println("Catch Statement")
        1
    } finally {
      if (isMailTrigger == "Y") {
        body = body + "\n\n"
        println("Sending mail ... ")

        import spark.implicits._
        val dfBody = List(body).toDF()

        if(fs.exists(new Path(logPath)))
          fs.delete(new Path(logPath),true)

        dfBody.repartition(1).write.csv(logPath)
        //sendMail(mails, targetTable, body)
      }
    }
  }

  def customQueryEmailBodyBuilder(customQuery: String, customExpectedValue: String, result: Boolean, actualOutput: String, customErrorType: String): String = {

    var body = "\n"
    body = body + "#Custom Query \n\n"
    body = body + "Validation Type : Custom Query \n"
    body = body + s"Validation Alert Type : ${alertMsg(customErrorType)} \n"
    body = body + s"Custom Query : ${customQuery} \n\n"
    body = body + s"Custom Query Expected Output : ${customExpectedValue} \n"
    if (result == true) body = body + "Result: Pass \n" else body = body + "Result: Fail \n"
    body = body + "\n ----------------------------------------------------------- \n"
    body
  }

  def customQueryEmailBodyBuilderExcp(customQuery: String, customExpectedValue: String, errorMsg: String): String = {

    var body = "\n"
    body = body + "#Custom Query \n\n"
    body = body + "Validation Type : Custom Query \n"
    body = body + s"Custom Query : ${customQuery} \n\n"
    body = body + s"Custom Query Expected Output : ${customExpectedValue} \n"
    body = body + s"Exception Msg: ${errorMsg} \n"
    body = body + "Result: Fail \n"
    body = body + "\n ----------------------------------------------------------- \n"
    body
  }

  def countCheckEmailBodyBuilder(result: Boolean, countErrorType: String): String = {
    var body = "\n"
    body = body + "#Count Check \n\n"
    body = body + "Validation Type : Count Check \n"
    body = body + s"Validation Alert Type : ${alertMsg(countErrorType)} \n"
    if (result == true) body = body + "Result: Pass \n" else body = body + "Result: Fail \n"
    body = body + "\n ----------------------------------------------------------- \n"
    body
  }

  def countCheckEmailBodyBuilderExcp(errorMsg: String): String = {
    var body = "\n"
    body = body + "#Count Check \n\n"
    body = body + "Validation Type : Count Check \n"
    body = body + s"Exception Msg: ${errorMsg} \n"
    body = body + "Result: Fail \n"
    body = body + "\n ----------------------------------------------------------- \n"
    body
  }

  def countMatchCheckEmailBodyBuilder(result: Boolean, countErrorType: String): String = {
    var body = "\n"
    body = body + "#Count Match Check \n\n"
    body = body + "Validation Type : Count Match Check \n"
    body = body + s"Validation Alert Type : ${alertMsg(countErrorType)} \n"
    if (result == true) body = body + "Result: Pass \n" else body = body + "Result: Fail \n"
    body = body + "\n ----------------------------------------------------------- \n"
    body
  }

  def countMatchCheckEmailBodyBuilderExcp(errorMsg: String): String = {
    var body = "\n"
    body = body + "#Count Match Check \n\n"
    body = body + "Validation Type : Count Match Check \n"
    body = body + s"Exception Msg: ${errorMsg} \n"
    body = body + "Result: Fail \n"
    body = body + "\n ----------------------------------------------------------- \n"
    body
  }

  def nullCheckEmailBodyBuilder(result: Boolean, cols: List[String], nulCols: String, nullErrorType: String): String = {
    var body = "\n"
    body = body + s"#Null Check for ${nulCols} \n\n"
    body = body + "Validation Type : Null Check \n"
    body = body + s"Validation Alert Type : ${alertMsg(nullErrorType)} \n"
    if (result == true) {
      body = body + "Null Columns: Non \n"
      body = body + "Result: Pass \n"
    } else {
      body = body + s"Null Columns: ${cols.mkString(",")} \n"
      body = body + "Result: Fail \n"
    }
    body = body + "\n ----------------------------------------------------------- \n"
    body
  }

  def nullCheckEmailBodyBuilderExcp(nulCols: String, errorMsg: String): String = {
    var body = "\n"
    body = body + s"#Null Check for ${nulCols} \n\n"
    body = body + "Validation Type : Null Check \n"
    body = body + s"Exception Msg: ${errorMsg} \n"
    body = body + "Result: Fail \n"
    body = body + "\n ----------------------------------------------------------- \n"
    body
  }

  def duplicateCheckEmailBodyBuilder(result: Boolean, count: Long, dupCols: String, duplicateErrorType: String): String = {
    var body = "\n"
    body = body + s"#Duplicate Check on ${dupCols} \n\n"
    body = body + "Validation Type : Duplicate Check \n"
    body = body + s"Validation Alert Type : ${alertMsg(duplicateErrorType)} \n"
    if (result == true) {
      body = body + "Total Duplicate Records: 0 \n"
      body = body + "Result: Pass \n"
    } else {
      body = body + s"Total Duplicate Records: ${count.toString} \n"
      body = body + "Result: Fail \n"
    }
    body = body + "\n ----------------------------------------------------------- \n"
    body
  }

  def duplicateCheckEmailBodyBuilderExcp(dupCols: String, errorMsg: String): String = {
    var body = "\n"
    body = body + s"#Duplicate Check on ${dupCols} \n\n"
    body = body + "Validation Type : Duplicate Check \n"
    body = body + s"Exception Msg: ${errorMsg} \n"
    body = body + "Result: Fail \n"
    body = body + "\n ----------------------------------------------------------- \n"
    body
  }

  def exasolMatchCheckEmailBodyBuilder(result: Boolean, countErrorType: String): String = {
    var body = "\n"
    body = body + "#Count Match Check For Exasol \n\n"
    body = body + "Validation Type : Count Match Check For Exasol\n\n"
    body = body + s"Validation Alert Type : ${alertMsg(countErrorType)} \n"
    if (result == true) body = body + "Result: Pass \n" else body = body + "Result: Fail \n"
    body = body + "\n ----------------------------------------------------------- \n"
    body
  }

  def exasolMatchCheckEmailBodyBuilderExcp(errorMsg: String): String = {
    var body = "\n"
    body = body + "#Count Match Check For Exasol \n\n"
    body = body + "Validation Type : Count Match Check For Exasol \n"
    body = body + s"Exception Msg: ${errorMsg} \n"
    body = body + "Result: Fail \n"
    body = body + "\n ----------------------------------------------------------- \n"
    body
  }

  def exceptionBody(errorMsg: String): String = {
    var body = "\n"
    body = body + s"Exception Msg: ${errorMsg} \n"
    body = body + "\n ----------------------------------------------------------- \n"
    body
  }
}