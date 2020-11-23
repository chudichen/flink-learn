package com.chudichen.util

import java.util.Properties

import com.chudichen.constant.PropertiesConstants
import org.apache.flink.api.java.utils.ParameterTool

/**
 * Kafka工具类
 *
 * @author chudichen
 * @since 2020-11-23
 */
object KafkaConfigUtil {

  def buildKafkaProps(): Properties = {
    buildKafkaProps(ParameterTool.fromSystemProperties())
  }

  def buildKafkaProps(parameterTool: ParameterTool): Properties = {
    val props = parameterTool.getProperties
    props.put("bootstrap.servers", parameterTool.get(PropertiesConstants.KAFKA_BROKERS, PropertiesConstants.DEFAULT_KAFKA_BROKERS))
    props.put("zookeeper.connect", parameterTool.get(PropertiesConstants.KAFKA_ZOOKEEPER_CONNECT, PropertiesConstants.DEFAULT_KAFKA_ZOOKEEPER_CONNECT))
    props.put("group.id", parameterTool.get(PropertiesConstants.KAFKA_GROUP_ID, PropertiesConstants.DEFAULT_KAFKA_GROUP_ID))
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")
    props
  }
}
