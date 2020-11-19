package com.chudichen.model

import scala.collection.mutable

/**
 * 单位事件
 *
 * @author chudichen
 * @since 2020-11-19
 */
case class MetricEvent(name: String, timestamp: Long, field: mutable.HashMap[String, Any], tags: mutable.HashMap[String, String])
