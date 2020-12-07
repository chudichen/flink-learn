package com.chudichen.examples.streaming.checkpoint

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.chudichen.examples.streaming.checkpoint.util.PvStatExactlyOnceKafkaUtil
import org.apache.flink.api.common.functions.{RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import org.apache.kafka.clients.consumer.ConsumerConfig

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * 代码中设置1分钟一次CheckPoint，CheckPoint语义EXACTLY ONCE,从Kafka中读取数据，
 * 这里为了化简代码，所以Kafka中读取的直接就是String类型的appId，按照appId的 keyBy
 * 之后执行RichFunction的open方法中会初始化ValueState[Long]类型的pvState，pvState
 * 就是状态信息，每次CheckPoint的时候，会把pvState的状态信息快照一份到hdfs来提供恢复。
 * 这里按照appId进行keyBy，所以没一个appId都会对应一个pvState，pvState里面存储着该
 * appId对应的pv值。
 * 每来一条数据都会执行一次map方法，当这条数据对应的appId是新app时，pvState里就没有存
 * 储这个appId当前的pv值，将来pv值赋值为1，当pvState里存储的value不为null时，拿出pv
 * 值+1后update到pvState中。
 *
 * map方法再将appId和pv值发送到下游算子，下游直接调用了print进行输出，这里完全可以替换
 * 成相应的RedisSink或HBaseSink，本案例中计算pv的工作交给了Flink内部的ValueState，
 * 不依赖外部存储介质进行累加，外部介质承担的角色仅仅是提供数据给业务方查询，所以无论下游
 * 使用什么形式的Sink，只要Sink端能够按照主键去重，该方案就可以保证ExactlyOnce
 *
 * @author chudichen
 * @since 2020-11-26
 */
object PvStatExactlyOnceApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 1分钟一次checkPoint
    env.enableCheckpointing(TimeUnit.MINUTES.toMillis(1))
    env.setParallelism(8)
    val checkpointConfig = env.getCheckpointConfig
    checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // Kafka配置
    val properties = new Properties()
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "app-pv-stat")

    val appKafkaConsumer = new FlinkKafkaConsumer011[String] (PvStatExactlyOnceKafkaUtil.topic, new SimpleStringSchema(), properties)
      .setStartFromLatest()
    env.addSource(appKafkaConsumer)
      .flatMap(new LocalKeyByFlatMap(10))
      // 按照appId进行keyBy
      .map(new CalculatePvFunction)
      .print()

    env.execute("Flink pv stat localKeyBy")
  }

  /**
   * LocalKeyByFlatMap中实现了在shuffle的上游端对数据进行预聚合，
   * 从而减少发送到下游的数据量，使得热点数据量大大降低。
   * 注：本案例中积攒批次使用数据量来积攒，当长时间数据量较少时，由于数据量积攒不够，
   * 可能导致上游buffer中数据不往下游发送，可以加定时策略，
   * 例如：如果数据量少但是时间超过了200ms，也会强制将数据发送到下游
   */
  class LocalKeyByFlatMap(var batchSize: Int) extends RichFlatMapFunction[String, (String, Long)] with CheckpointedFunction {

    /**
     * 由于加了buffer，所以Checkpoint的时候，
     * 可能还有Checkpoint之前的数据缓存在buffer中没有发送到下游被处理
     * 把者部分数据放到localPvStatListState中，当Checkpoint恢复时，
     * 把这部分数据从状态中恢复到buffer中
     */
    private var localPvStatListState: ListState[(String, Long)] = _

    /**
     * 本地buffer，存放local端缓存的app的pv信息
     */
    private val localPvStat = mutable.HashMap[String, Long]()

    /**
     * 计数器，获取当前批次接收的数据量
     */
    private var currentSize: Int = _

    private var subtaskIndex: Int = _

    override def flatMap(value: String, out: Collector[(String, Long)]): Unit = {
      // 将新来的数据添加到buffer中
      val pv = localPvStat.getOrElse(value, 0L)
      localPvStat.put(value, pv + 1)
      println("invoke subtask: " + subtaskIndex + " appId: " + value + " pv: " + localPvStat.get(value))

      // 如果到达设定的批次，则将buffer中的数据发送到下游
      if (currentSize >= batchSize) {
        localPvStat.foreach(entry => {
          out.collect((entry._1, entry._2))
          println("batchSend subtask: " + subtaskIndex + " appId: " + entry._1 + " PV: " + entry._2)
          localPvStat.clear()
          currentSize = 0
        })
      }
    }

    override def snapshotState(context: FunctionSnapshotContext): Unit = {
      // 将buffer中的数据保存到状态中，来保证Exactly Once
      localPvStatListState.clear()
      localPvStat.foreach(entry => {
        localPvStatListState.add((entry._1, entry._2))
        println("snapshot subtask: " + subtaskIndex + " appId: " + entry._1 + " pv: " + entry._2)
      })
    }

    override def initializeState(context: FunctionInitializationContext): Unit = {
      subtaskIndex = getRuntimeContext.getIndexOfThisSubtask
      // 从状态中恢复buffer中的数据
      localPvStatListState = context.getOperatorStateStore.getListState(new ListStateDescriptor[(String, Long)](
        "localPvStat",
        TypeInformation.of(new TypeHint[(String, Long)] {})))
      if (context.isRestored) {
        // 从状态中回复buffer中的数据
        localPvStatListState.get().asScala.foreach(entry => {
          val pv = localPvStat.getOrElse(entry._1, 0L)
          // 如果出现了pv ！= 0， 说明改变了并行度，
          // ListState中的数据会被均匀分发到新的subtask中
          // 所以单个subtask恢复的状态中可能包含两个相同的app的数据
          localPvStat.put(entry._1, pv + entry._2)
          println("init subtask: " + subtaskIndex + " appId: " + entry._1 + " pv: " + entry._2)
        })
        // 从状态恢复时，默认
        currentSize = batchSize
      }
    }
  }

  class CalculatePvFunction extends RichMapFunction[(String, Long), (String, Long)] {

    private var pvState: ValueState[Long] = _
    private var pv = 0L

    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 初始化状态
      pvState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("pvStat", TypeInformation.of(new TypeHint[Long] {})))
    }

    override def map(value: (String, Long)): (String, Long) = {
      // 从状态中获取该app的pv值，加上新收到的pv值以后，update到状态中
      if (null == pvState) {
        println(value._1 + " is new, PV is " + value._2)
        pv = value._2
      } else {
        pv = pvState.value()
      }
      value
    }
  }
}
