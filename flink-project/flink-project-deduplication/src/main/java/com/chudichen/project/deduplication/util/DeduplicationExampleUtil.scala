package com.chudichen.project.deduplication.util

import java.util.{Properties, UUID}

import com.chudichen.common.util.GsonUtil
import com.chudichen.project.deduplication.model.UserVisitWebEvent
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.joda.time.DateTime

import scala.util.Random

/**
 * @author chudichen
 * @since 2020-11-24
 */
object DeduplicationExampleUtil {

  val broker_list = "127.0.0.1:9092"

  /** kafka topic flink 程序中需要和这个统一 */
  val topic = "user-visit-log-topic"

  val random = new Random()

  def writeToKafka(): Unit = {
    val props = new Properties
    props.put("bootstrap.servers", broker_list)
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    // 生成0-9的随机数作为appId
    for (_ <- 1 to 10) {
      val id = UUID.randomUUID().toString
      val yyyyMMdd = new DateTime(System.currentTimeMillis()).toString("yyyyMMdd")
      val pageId = random.nextInt(10)
      val userId = random.nextInt(100)

      val userVisitWebEvent = UserVisitWebEvent(id, yyyyMMdd, pageId, userId.toString, "url/" + pageId)
      val record = new ProducerRecord[String, String](topic, null, null, GsonUtil.toJson(userVisitWebEvent))
      producer.send(record)
      println("发送数据： " + GsonUtil.toJson(userVisitWebEvent))
    }
    producer.flush()
  }

  def main(args: Array[String]): Unit = {
    while (true) {
      Thread.sleep(100);
      writeToKafka();
    }
  }
}
