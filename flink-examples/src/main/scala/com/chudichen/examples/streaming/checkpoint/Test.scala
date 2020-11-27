package com.chudichen.examples.streaming.checkpoint

import org.apache.flink.api.common.state.ValueState

/**
 * @author chudichen
 * @since 2020-11-27
 */
object Test {

  private var pvState: ValueState[Long] = _

  def main(args: Array[String]): Unit = {
    println(pvState)
  }
}
