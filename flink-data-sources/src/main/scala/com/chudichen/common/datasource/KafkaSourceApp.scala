package com.chudichen.common.datasource

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011


/**
 * 利用flink自带的kafka source读取kafka数据
 *
 * @author chudichen
 * @since 2020-11-19
 */
object KafkaSourceApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val props = new Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("zookeeper.connect", "localhost:2181")
    props.put("group.id", "metric-group")
    // key反序列化
    props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    // value反序列化
    props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("auto.offset.reset", "latest")

    val datasourceSource = env.addSource(new FlinkKafkaConsumer011[String](
      "metric",
      new SimpleStringSchema(),
      props
    )).setParallelism(1)

    // 把从kafka读取的数据打印在控制台上
    datasourceSource.print()

    env.execute("Flink add data source")
  }
}
