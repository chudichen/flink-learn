package com.chudichen.util

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import com.chudichen.constant.PropertiesConstants
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.TimeCharacteristic

/**
 * @author chudichen
 * @since 2020-11-19
 */
object ExecutionEnvUtil {

  def createParameterTool(args: Array[String]): ParameterTool = {
    ParameterTool.fromPropertiesFile(ExecutionEnvUtil.getClass.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
      .mergeWith(ParameterTool.fromArgs(args))
      .mergeWith(ParameterTool.fromSystemProperties())
  }

  def prepare(parameterTool: ParameterTool): StreamExecutionEnvironment = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM, 5))
    env.getConfig.disableSysoutLogging()
    env.getConfig.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 60000))
    if (parameterTool.getBoolean(PropertiesConstants.STREAM_CHECKPOINT_ENABLE, true)) {
      env.enableCheckpointing(parameterTool.getLong(PropertiesConstants.STREAM_CHECKPOINT_ENABLE, 10000))
    }
    env.getConfig.setGlobalJobParameters(parameterTool)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env
  }

  private def createParameterTool(): ParameterTool = {
    try {
      ParameterTool.fromPropertiesFile(ExecutionEnvUtil.getClass.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME))
        .mergeWith(ParameterTool.fromSystemProperties())
    } catch {
      case e:Exception => e.printStackTrace()
    }
    ParameterTool.fromSystemProperties()
  }
}
