package com.chudichen.examples.streaming.async

import java.util
import java.util.Collections
import java.util.concurrent.{ExecutorService, Executors, ThreadLocalRandom, TimeUnit}

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.configuration.Configuration

import scala.collection.JavaConversions._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.datastream.AsyncDataStream
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.async.{AsyncFunction, ResultFuture, RichAsyncFunction}
import org.apache.flink.util.ExecutorUtils

/**
 * 异步
 *
 * @author chudichen
 * @since 2020-11-20
 */
object AsyncIOExample {

  private val EXACTLY_ONCE_MODE = "exactly_once"
  private val EVENT_TIME = "EventTime"
  private val INGESTION_TIME = "IngestionTime"
  private val ORDERED = "ordered"

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)

    var statePath:String = null
    var cpMode:String = null
    var maxCount = 0
    var sleepFactor = 0L
    var failRatio = 0F
    var mode:String = null
    var taskNum = 0
    var timeType:String = null
    var shutdownWaitTS = 0L
    var timeout = 0L

    try { // check the configuration for the job
      statePath = params.get("fsStatePath", null)
      cpMode = params.get("checkpointMode", "exactly_once")
      maxCount = params.getInt("maxCount", 100000)
      sleepFactor = params.getLong("sleepFactor", 100)
      failRatio = params.getFloat("failRatio", 0.001f)
      mode = params.get("waitMode", "ordered")
      taskNum = params.getInt("waitOperatorParallelism", 1)
      timeType = params.get("eventType", "EventTime")
      shutdownWaitTS = params.getLong("shutdownWaitTS", 20000)
      timeout = params.getLong("timeout", 10000L)
    } catch {
      case e: Exception =>
        throw e
    }

    if (statePath != null) {
      env.setStateBackend(new FsStateBackend(statePath))
    }

    if (EXACTLY_ONCE_MODE.equals(cpMode)) {
      env.enableCheckpointing(1000L, CheckpointingMode.EXACTLY_ONCE)
    } else {
      env.enableCheckpointing(1000L, CheckpointingMode.AT_LEAST_ONCE)
    }

    val source = new SimpleSource()
    source.setCounter(maxCount)
    val inputStream = env.addSource(source)

//    val function:AsyncFunction [Object, String] = new SampleAsyncFunction
//    function.setSleepFactor(sleepFactor)
//    function.setFailRatio(failRatio)
//    function.setShutDownWaitTS(shutdownWaitTS)

//    var result:DataStream[String] = null
//    if (ORDERED.equals(mode)) {
//      result = AsyncDataStream.orderedWait(
//        inputStream,
//        function,
//        timeout,
//        TimeUnit.MILLISECONDS,
//        20
//      )
//    }

    env.execute("Async IO Example")
  }

  class SimpleSource extends SourceFunction[Integer] with ListCheckpointed[Integer] {
    private var isRunning = true
    private var counter = 0
    private var start = 0

    def setCounter(count: Int):Unit = {
      this.counter = count
    }

    override def run(ctx: SourceFunction.SourceContext[Integer]): Unit = {
      while ((start < counter || counter == -1) && isRunning) {
        ctx.getCheckpointLock.synchronized {
          ctx.collect(start)
          start += 1
          if (start == Integer.MAX_VALUE) {
            start = 0
          }
          Thread.sleep(10L)
        }
      }
    }

    override def cancel(): Unit = {
      isRunning = false
    }

    override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Integer] = {
      Collections.singletonList(start)
    }

    override def restoreState(state: util.List[Integer]): Unit = {
      state.foreach((i: Integer) => this.start = i)
    }
  }

  class SampleAsyncFunction extends RichAsyncFunction[Integer, String] {

    private var executorService: ExecutorService = _
    private var sleepFactor:Long = _
    private var failRatio:Float = _
    private var shutdownWaitTS:Long = _

    def setSleepFactor(sleep: Long):Unit = {
      sleepFactor = sleep
    }

    def setFailRatio(fail: Float):Unit = {
      failRatio = fail
    }

    def setShutDownWaitTS(wait: Long):Unit = {
      shutdownWaitTS = wait
    }


    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      executorService = Executors.newFixedThreadPool(30)
    }

    override def close(): Unit = {
      super.close()
      ExecutorUtils.gracefulShutdown(shutdownWaitTS, TimeUnit.MICROSECONDS, executorService)
    }

    override def asyncInvoke(input: Integer, resultFuture: ResultFuture[String]): Unit = {
      executorService.submit(new Runnable {
        override def run(): Unit = {
          val sleep = (ThreadLocalRandom.current().nextFloat() * sleepFactor).toLong
          try {
            Thread.sleep(sleep)
            if (ThreadLocalRandom.current().nextFloat() < failRatio) {
              resultFuture.completeExceptionally(new Exception("error"))
            } else {
              resultFuture.complete(
                Collections.singletonList("key-" + (input % 10))
              )
            }
          } catch {
            case e: InterruptedException =>
              resultFuture.complete(new util.ArrayList[String](0))
          }
        }
      })
    }
  }
}
