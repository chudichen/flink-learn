package com.chudichen.schema

import com.chudichen.model.MetricEvent
import org.apache.flink.api.common.serialization.{DeserializationSchema, SerializationSchema}

/**
 * @author chudichen
 * @since 2020-11-19
 */
trait Schema extends DeserializationSchema[MetricEvent] with SerializationSchema[MetricEvent]{

}
