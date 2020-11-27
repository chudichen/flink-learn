package com.chudichen.examples.streaming.checkpoint.util

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable
import scala.util.Random

/**
 * 用于给PvStatExactlyOnce生成数据，并统计数据集中不同数据的个人
 *
 * @author chudichen
 * @since 2020-11-26
 */
object PvStatExactlyOnceKafkaUtil {

  val broker_list = "localhost:9092"
  val topic = "app-topic"
  private val producerMap = mutable.HashMap[String, Long]()

  def main(args: Array[String]): Unit = {
    while (true) {
      Thread.sleep(1000)
      writeToKafka()
    }
  }

  def writeToKafka(): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", broker_list)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](properties)

    // 生成0-9的随机数作为appId
    val value = Random.nextInt(10).toString
    val record = new ProducerRecord[String, String](topic, null, null, value)
    producer.send(record)
    println("发送数据： " + record)

    val pv = producerMap.get(value)
    if (pv.isEmpty) {
      producerMap.put(value, 1L)
    } else {
      producerMap.put(value, pv.map(_ + 1).get)
    }

    producerMap.foreach(entry => println("appId: " + entry._1 + " count: " + entry._2))
    producer.flush()
  }
}
