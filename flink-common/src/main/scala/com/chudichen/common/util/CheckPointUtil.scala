package com.chudichen.common.util

import java.net.URI

import com.chudichen.common.constant.PropertiesConstants._
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.StateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * checkPoint工具类
 *
 * @author chudichen
 * @since 2020-11-25
 */
object CheckPointUtil {

  def setCheckpointConfig(env: StreamExecutionEnvironment, parameterTool: ParameterTool): StreamExecutionEnvironment = {
    if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE, false) && CHECKPOINT_MEMORY.equals(parameterTool.get(STREAM_CHECKPOINT_TYPE).toLowerCase())) {
      // 1. state 存放在内存中默认是5M
      val stateBackend = new MemoryStateBackend(5 * 1024 * 1024)
      env.setStateBackend(stateBackend)
    }

    if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE, false) && CHECKPOINT_FS.equals(parameterTool.get(STREAM_CHECKPOINT_TYPE).toLowerCase())) {
      val stateBackend = new FsStateBackend(new URI(parameterTool.get(STREAM_CHECKPOINT_DIR)), 0)
      env.setStateBackend(stateBackend)
    }

    if (parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE, false) && CHECKPOINT_ROCKETSDB.equals(parameterTool.get(STREAM_CHECKPOINT_TYPE).toLowerCase())) {
      val rocksDBStateBackend = new RocksDBStateBackend(parameterTool.get(STREAM_CHECKPOINT_DIR))
      env.setStateBackend(rocksDBStateBackend)
    }

    // 设置checkpoint周期时间
    env.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL, 60000))
    // 高级设置（这些配置也建议携程配置文件中去读取，优先环境变量）
    // 设置 exactly-once 模式
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 设置checkpoint最小间隔500ms
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    // 设置checkpoint必须在1分钟内完成，否则会被丢弃
    env.getCheckpointConfig.setCheckpointTimeout(60000)
    // 设置checkpoint失败时，任务不会fail，该checkpoint会被丢弃
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    // 设置checkpoint的并发度为1
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    env
  }
}
