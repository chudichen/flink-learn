package com.chudichen.datasource.util

import java.util.Properties

import com.chudichen.common.model.MetricEvent
import com.chudichen.common.util.GsonUtil
import com.chudichen.model.MetricEvent
import com.chudichen.util.GsonUtil
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.collection.mutable

/**
 * 往Kafka中写数据
 *
 * @author chudichen
 * @since 2020-11-19
 */
object KafkaUtil {

  val brokerList = "localhost:9092"
  val topic = "metric"

  def writeToKafka():Unit = {
    val props = new Properties()
    props.put("bootstrap.servers", brokerList)
    // key的序列化
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // value的序列化
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    val fields = new mutable.HashMap[String, Any]()
    fields.put("used_percent", 90D)
    fields.put("max", 27244873D)
    fields.put("used", 17244873D)
    fields.put("init", 27244873D)

    val tags = new mutable.HashMap[String, String]()
    tags.put("cluster", "chudichen")
    tags.put("host_ip", "127.0.0.1")

    val name = "mem"
    val timestamp = System.currentTimeMillis()
    val metric = MetricEvent(name, timestamp, fields, tags)

    val record = new ProducerRecord[String, String](topic, null, null, GsonUtil.toJson(metric))
    producer.send(record)

    println(record)

    producer.flush()
  }

  /**
   * 发送程序
   *
   * @param args 参数
   */
  def main(args: Array[String]): Unit = {
    while (true) {
      Thread.sleep(300)
      writeToKafka()
    }
  }
}
