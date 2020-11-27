package com.chudichen.examples

import com.chudichen.examples.util.{SensorReading, SensorSource, SensorTimeAssigner}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

/**
 * @author chudichen
 * @since 2020-11-23
 */
object App {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(1000L)

    val sensorData: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    val avgTemp: DataStream[SensorReading] = sensorData
        .map((r: SensorReading) =>
          SensorReading(r.id, r.timestamp, (r.temperature - 32) * (5.0 / 9.0)))
        .keyBy(_.id)
        .timeWindow(Time.seconds(1))
        .apply(new TemperatureAveranger)

    avgTemp.print()
    env.execute("COmpute average sensor temperature")
  }

  class TemperatureAveranger extends WindowFunction[SensorReading, SensorReading, String, TimeWindow] {

    override def apply(key: String, window: TimeWindow, input: Iterable[SensorReading], out: Collector[SensorReading]): Unit = {
      val (cnt, sum) = input.foldLeft((0, 0.0))((c, r) => (c._1 + 1, c._2 + r.temperature))
      val avgTemp = sum / cnt

      out.collect(SensorReading(key, window.getEnd, avgTemp))
    }
  }
}
