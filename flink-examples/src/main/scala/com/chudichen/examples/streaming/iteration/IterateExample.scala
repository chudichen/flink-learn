package com.chudichen.examples.streaming.iteration

import com.jayway.jsonpath.MapFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

/**
 * 迭代
 *
 * @author chudichen
 * @since 2020-12-10
 */
object IterateExample {

  val BOUND = 100

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment.setBufferTimeout(1)
    val it = env.addSource(new RandomFibonacciSource)
      .map(data => (data._1, data._2, data._1, data._2, 0))

    it.map(data => (data._1, data._2, data._2, data._1 + data._2, data._3 + 1))
      .map(data => {
          if (data._3 < BOUND && data._4 < BOUND) {
            ("iterate", data)
          } else {
            ("output", data)
          }
        })
      .keyBy(_._1)
      .filter(_._1.equals("output"))
      .print()

    env.execute("Streaming Iteration Example")
  }

  class RandomFibonacciSource extends SourceFunction[(Int, Int)] {

    var isRunning = true
    var counter:Int = _

    override def run(ctx: SourceFunction.SourceContext[(Int, Int)]): Unit = {
      while (isRunning && counter < BOUND) {
        val first = Random.nextInt(BOUND / 2 - 1) + 1
        val second = Random.nextInt(BOUND / 2 - 1) + 1
        ctx.collect((first, second))
        counter += 1
        Thread.sleep(50L)
      }
    }

    override def cancel(): Unit = {
      isRunning = false
    }
  }

}
