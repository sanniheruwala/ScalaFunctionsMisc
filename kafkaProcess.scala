package com.rxcorp.bdf.datalake.de9.trxAsset

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Properties}

import com.rxcorp.bdf.datalake.de9.utils.ImpalaCon
import com.rxcorp.bdf.datalake.de9.utils.ImpalaCon.updateImpalaStats
import com.rxcorp.bdf.datalake.de9.utils.Logger.logger
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.SparkSession

import scala.collection.JavaConverters._
import scala.util.parsing.json.JSONObject

/**
  * Created by SHeruwala on 12/20/2018.
  * spark2-shell --queue de9 --packages org.apache.kafka:kafka_2.11:1.1.1
  */
object kafkaProcess {

  val props: Config = ConfigFactory.load()
  val offset: String = props.getString("CH2_KAFKA_OFFSET") //"earliest"
  val autoCommit: String = props.getString("CH2_KAKFA_AUTO_COMMIT")
  val consumerTopic: String = props.getString("CH2_CONSUMER_TOPIC") // "devl-dct-publish-evt"
  val publisherTopic: String = props.getString("CH2_PRODUCER_TOPIC")
  val brokerServer: String = props.getString("CH2_BROKER_SERVER") //"kafka-0-broker.uacc-pex-confluentkafka4.cdt.dcos:9615,kafka-1-broker.uacc-pex-confluentkafka4.cdt.dcos:9615,kafka-2-broker.uacc-pex-confluentkafka4.cdt.dcos:9615"

  def getCurrentdateTimeStamp: Timestamp = {
    val today: java.util.Date = Calendar.getInstance.getTime
    val timeFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val now: String = timeFormat.format(today)
    val re: Timestamp = java.sql.Timestamp.valueOf(now)
    re
  }

  /**
    * Kafka procedure for consumer.
    *
    * @param spark     Spark Context
    * @param process   consumer
    * @param tableName de9_ch2_metadata_store
    * @param db        devl_de9
    * @return success/failure
    */
  def runKafkaConsumer(spark: SparkSession, process: String, tableName: String, db: String): Int = {

    try {

      println(spark.catalog.clearCache())

      val run: Int = process match {
        case "consumer" =>

          var output: Seq[String] = Seq()
          val TOPIC: String = consumerTopic

          val props: Properties = new Properties()
          props.put("bootstrap.servers", brokerServer)
          props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
          props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
          props.put("group.id", "Kafka-test")
          props.put("auto.offset.reset", offset) // latest - for previous records (auto.offset.reset: String must be one of: latest, earliest, none)
          props.put("enable.auto.commit", "true")
          val consumer: KafkaConsumer[String, String] = new KafkaConsumer[String, String](props)
          consumer.subscribe(util.Collections.singletonList(TOPIC))

          /**
            * Handle error message and write it to log file.
            */

          for (i <- 0 to 100) {
            val records: ConsumerRecords[String, String] = consumer.poll(1)
            for (record <- records.asScala) {
              try {
                val data: Map[String, Any] = scala.util.parsing.json.JSON.parseRaw(record.value()).getOrElse().asInstanceOf[JSONObject].obj
                if (data.get("DefinitionType").get.toString == "GEOGRAPHY") {
                  val defId: String = data.get("DefinitionID").get.toString.split("\\.")(0)
                  output = output :+ defId
                }
              }catch{
                case ex: Throwable => ex.printStackTrace()
                  println(ex.getMessage)
                  logger.error("Message is not readable : "+record,ex)
              }
            }
          }

          val curr_ts: Timestamp = getCurrentdateTimeStamp
          output.distinct.foreach(x => ImpalaCon.impalaQuery(s"insert into ${db}.${tableName} select '${x}','${curr_ts}','QUEUED',null;"))
          updateImpalaStats(db, tableName)
          println(spark.catalog.clearCache())

          consumer.close()

          0
      }
      run
    } catch {
      case ex: Throwable => ex.printStackTrace()
        println(ex.getMessage)
        1
    }
  }

  /**
    * Kafka procedure for producer.
    *
    * @param spark SparkContext
    * @param process producer
    * @param cycleId i.e. 201811
    * @param cycleType Weekly/Monthly
    * @param db devl_de9
    * @return Success/Failure
    */
  def runKafkaProducer(spark: SparkSession, process: String, cycleId: String,cycleType:String, db: String): Int = {

    try {

      println(spark.catalog.clearCache())

      val run: Int = process match {
        case "producer" =>

          val TOPIC: String = publisherTopic

          val props: Properties = new Properties()
          props.put("bootstrap.servers", brokerServer)
          props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
          props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

          val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](props)

          logger.info("Kafka producer context created.")

          val curr_ts: Timestamp = getCurrentdateTimeStamp
          val ISOCountryCode: String = "DE"
          val subjectArea: String = "SELLOUT"
          val jsonStr: String = s"""{\"timestamp\": \"$curr_ts\",\"ISOCountryCode\": \"$ISOCountryCode\",\"subjectArea\": \"$subjectArea\",\"cycleId\": \"$cycleId\" ,\"cycleType\": \"$cycleType\"}"""

          logger.info("Trying to send message to P360 : " + jsonStr)

          val record: ProducerRecord[String, String] = new ProducerRecord(TOPIC, "sellout", jsonStr)
          producer.send(record)

          logger.info("Message sent to P360.")

          producer.close()

          logger.info("Producer close.")
          0
      }
      run
    } catch {
      case ex: Throwable => ex.printStackTrace()
        println(ex.getMessage)
        1
    }
  }

}
