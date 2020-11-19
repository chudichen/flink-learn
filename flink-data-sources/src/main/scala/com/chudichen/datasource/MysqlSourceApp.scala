package com.chudichen.datasource

import com.chudichen.datasource.source.SourceFromMysql
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * 从Mysql中读取数据
 *
 * @author chudichen
 * @since 2020-11-19
 */
object MysqlSourceApp {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.addSource(new SourceFromMysql).print()
    env.execute("Flink add data source")
  }
}
