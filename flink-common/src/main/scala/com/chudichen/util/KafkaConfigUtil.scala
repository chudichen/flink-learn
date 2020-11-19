package com.chudichen.util

import java.util.Properties

import com.chudichen.constant.PropertiesConstants
import org.apache.flink.api.java.utils.ParameterTool

/**
 * Kafka配置工具类
 *
 * @author chudichen
 * @since 2020-11-19
 */
object KafkaConfigUtil {

  def buildKafkaProps(): Properties = {

  }

  def buildKafkaProps(parameterTool: ParameterTool): Properties = {
    val props = parameterTool.getProperties
    props.put("bootstrap.servers", parameterTool.get(PropertiesConstants.KAFKA_BROKERS, ""))
  }
}
