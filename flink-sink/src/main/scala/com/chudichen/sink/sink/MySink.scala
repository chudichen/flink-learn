package com.chudichen.sink.sink

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * @author chudichen
 * @since 2020-11-23
 */
class MySink(var tx: String) extends RichSinkFunction[String] {

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    tx = "5"
    println("==============")
  }

  override def invoke(value: String, context: SinkFunction.Context[_]): Unit = {
    println(value + " " + tx)
  }
}
