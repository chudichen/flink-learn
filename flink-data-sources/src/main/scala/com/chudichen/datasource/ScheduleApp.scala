package com.chudichen.datasource

import java.util.concurrent.{Executors, TimeUnit}

import com.chudichen.common.datasource.util.MysqlUtil
import com.chudichen.common.util.ExecutionEnvUtil
import com.chudichen.datasource.model.Rule
import com.chudichen.datasource.util.MysqlUtil
import com.chudichen.util.ExecutionEnvUtil

import scala.collection.mutable.ListBuffer

/**
 * 定时警告
 *
 * @author chudichen
 * @since 2020-11-19
 */
object ScheduleApp {

  def main(args: Array[String]): Unit = {
    // 定时捞取规则，每隔一分钟捞取一次
    val threadPool = Executors.newScheduledThreadPool(1)
    threadPool.scheduleAtFixedRate(new GetRulesJob(),0 , 1, TimeUnit.MINUTES)

    val parameterTool = ExecutionEnvUtil.createParameterTool(args)
    val env = ExecutionEnvUtil.prepare(parameterTool)

  }

  class GetRulesJob extends Runnable {
    override def run(): Unit = {
      try {
        getRules()
      } catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }

  def getRules(): ListBuffer[Rule] = {
    println("------get rule")
    val sql = "select * from rule"

    val connection = MysqlUtil.getConnection("com.mysql.cj.jdbc.Driver",
      "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8",
    "root",
      "root")
    val ps = connection.prepareStatement(sql)
    val resultSet = ps.executeQuery()

    val list = new ListBuffer[Rule]()

    while (resultSet.next()) {
      list.append(Rule(
        resultSet.getString("id"),
        resultSet.getString("name"),
        resultSet.getString("type"),
        resultSet.getString("measurement"),
        resultSet.getString("threshold"),
        resultSet.getString("level"),
        resultSet.getString("target_type"),
        resultSet.getString("target_id"),
        resultSet.getString("target_id"),
        resultSet.getString("webhook")
      ))
    }
    list
  }
}
