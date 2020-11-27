package com.chudichen.datasource.source

import java.sql.{Connection, PreparedStatement}

import com.chudichen.common.datasource.util.MysqlUtil
import com.chudichen.datasource.model.Student
import com.chudichen.datasource.util.MysqlUtil
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

/**
 * @author chudichen
 * @since 2020-11-19
 */
class SourceFromMysql extends RichParallelSourceFunction[Student] {

  var connection: Connection = _
  var ps: PreparedStatement = _


  /**
   * 在open（）中建立链接，这样不用每次invoke时候都要建立和释放链接
   *
   * @param parameters 参数
   */
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    connection = MysqlUtil.getConnection(
      "com.mysql.cj.jdbc.Driver",
      "jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8",
      "root",
      "root")
    val sql = "select * from student"
    ps = this.connection.prepareStatement(sql)
  }

  /**
   * 程序执行完毕就可以进行关闭链接和释放资源的动作了
   */
  override def close(): Unit = {
    super.close()
    if (connection != null) {
      connection.close()
    }

    if (ps != null) {
      ps.close()
    }
  }

  /**
   * DataStream 调用一次run()方法来获取数据
   *
   * @param ctx 上下文
   */
  override def run(ctx: SourceFunction.SourceContext[Student]): Unit = {
    val resultSet = ps.executeQuery()
    while (resultSet.next()) {
      val student = Student(
        resultSet.getInt("id"),
        resultSet.getString("name"),
        resultSet.getString("password"),
        resultSet.getInt("age")
      )
      ctx.collect(student)
    }
  }

  override def cancel(): Unit = {}
}
