package com.chudichen.common.sink

import com.chudichen.common.sink.sink.MySink
import org.apache.flink.streaming.api.scala._

/**
 * @author chudichen
 * @since 2020-11-23
 */
object MySinkApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val source = env.socketTextStream("127.0.0.1", 9000)
    source.addSink(new MySink("6")).setParallelism(5)
    env.execute("my sink app")
  }
}
