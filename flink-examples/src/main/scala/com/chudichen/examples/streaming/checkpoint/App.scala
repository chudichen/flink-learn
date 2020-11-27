package com.chudichen.examples.streaming.checkpoint

import com.chudichen.common.constant.PropertiesConstants
import com.chudichen.common.model.MetricEvent
import com.chudichen.common.util.{CheckPointUtil, ExecutionEnvUtil, KafkaConfigUtil}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.json4s._
import org.json4s.jackson.JsonMethods._

/**
 * @author chudichen
 * @since 2020-11-25
 */
object App {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val parameterTool = ExecutionEnvUtil.PARAMETER_TOOL
    val props = KafkaConfigUtil.buildKafkaProps(parameterTool)
    val source = new FlinkKafkaConsumer011[String](
      parameterTool.get(PropertiesConstants.METRICS_TOPIC, PropertiesConstants.CHUDICHEN),
      new SimpleStringSchema(),
      props)

    val metricData = env.addSource(source).map((str: String) => {
      implicit val formats: DefaultFormats.type = DefaultFormats
      parse(str).extract[MetricEvent]})

    metricData.print()

    CheckPointUtil.setCheckpointConfig(env, parameterTool)
      .execute("checkpoint config example")
  }
}
