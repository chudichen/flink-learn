package com.chudichen.project.deduplication

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.chudichen.project.deduplication.model.UserVisitWebEvent
import com.chudichen.project.deduplication.util.DeduplicationExampleUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.contrib.streaming.state.{PredefinedOptions, RocksDBStateBackend}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.json4s._
import org.json4s.jackson.JsonMethods._


/**
 * @author chudichen
 * @since 2020-11-24
 */
object KeyedStateDeduplication {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(6)

    // 使用RocksDBStateBackend作为状态后端，并开启增量CheckPoint
    val rocksDBStateBackend = new RocksDBStateBackend("hdfs:///flink/checkpoints", true)
    rocksDBStateBackend.setNumberOfTransferThreads(3)
    // 设置为机械硬盘+内存模式
    rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM)
    rocksDBStateBackend.enableTtlCompactionFilter()
    env.setStateBackend(rocksDBStateBackend)

    // Checkpoint 间隔为10分钟
    env.enableCheckpointing(TimeUnit.MINUTES.toMillis(10))

    // 配置Checkpoint
    val checkpointConf = env.getCheckpointConfig
    checkpointConf.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    checkpointConf.setMinPauseBetweenCheckpoints(TimeUnit.MINUTES.toMillis(8))
    checkpointConf.setCheckpointTimeout(TimeUnit.MINUTES.toMillis(20))
    checkpointConf.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // Kafka consumer配置
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, DeduplicationExampleUtil.broker_list)
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "keyed-state- deduplication")
    val kafkaConsumer = new FlinkKafkaConsumer011[String](DeduplicationExampleUtil.topic, new SimpleStringSchema(), props)
      .setStartFromGroupOffsets()

    env.addSource(kafkaConsumer)
      .map(value => {
        //导入隐式值
        implicit val formats: DefaultFormats.type = DefaultFormats
        parse(value).extract[UserVisitWebEvent]
      }).keyBy(_.id)
        .addSink(new KeyedStateSink)

    env.execute("keyedStateDeduplication")
  }

  /**
   * 用来维护实现百亿去重逻辑的算子
   */
  class KeyedStateSink extends RichSinkFunction[UserVisitWebEvent] {
    // 使用ValueState来标示当前key是否存在过
    private var isExist: ValueState[Boolean] = _

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      val keyedStateDuplicated: ValueStateDescriptor[Boolean] = new ValueStateDescriptor[Boolean]("KeyedStateDeduplication",
        TypeInformation.of(new TypeHint[Boolean]() {}))

      // 状态TTL相关配置，过期时间设定为36小时
      val ttlConfig = StateTtlConfig
        .newBuilder(Time.hours(36))
        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
        .cleanupInRocksdbCompactFilter(50000000L)
        .build()
      // 开启TTL
      keyedStateDuplicated.enableTimeToLive(ttlConfig)
      isExist = getRuntimeContext.getState(keyedStateDuplicated)
    }

    /**
     * 当key第一次出现的时候，isExist.value()会返回false
     * key第一次出现，说明之前没有被处理过，
     * 此时应该执行正常代码的逻辑，并给状态isExist赋值，标示当前key
     * 已经被处理过了。下次再有相同的主键时，isExist.value()就不会为null了
     *
     * @param value 数值
     * @param context 上下文
     */
    override def invoke(value: UserVisitWebEvent, context: SinkFunction.Context[_]): Unit = {
      if (!isExist.value()) {
        isExist.update(true)
      }
    }
  }
}
