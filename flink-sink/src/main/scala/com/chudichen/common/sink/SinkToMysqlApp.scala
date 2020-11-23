package com.chudichen.common.sink

import com.chudichen.common.sink.model.Student
import com.chudichen.common.sink.sink.SinkToMySql
import com.chudichen.common.util.{ExecutionEnvUtil, GsonUtil, KafkaConfigUtil}
import com.chudichen.util.{ExecutionEnvUtil, GsonUtil, KafkaConfigUtil}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import com.chudichen.constant.PropertiesConstants.METRICS_TOPIC
import com.chudichen.sink.model.Student
import com.chudichen.sink.sink.SinkToMySql
import org.apache.flink.api.common.serialization.SimpleStringSchema


/**
 * @author chudichen
 * @since 2020-11-23
 */
object SinkToMysqlApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val parameterTool = ExecutionEnvUtil.PARAMETER_TOOL
    val props = KafkaConfigUtil.buildKafkaProps(parameterTool)
    val source = new FlinkKafkaConsumer011[String](
      parameterTool.get(METRICS_TOPIC),
      new SimpleStringSchema(),
      props
    )

    val dataStream = env.addSource(source).setParallelism(5)
    val students: DataStream[Student] = dataStream.map(GsonUtil.fromJson(_, Student.getClass))

    students.addSink(new SinkToMySql())
    env.execute("Flink data sink to Mysql")
  }
}
