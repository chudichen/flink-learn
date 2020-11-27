package com.chudichen.examples.util

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @author chudichen
 * @since 2020-11-23
 */
class SensorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
  override def extractTimestamp(element: SensorReading): Long = element.timestamp
}
