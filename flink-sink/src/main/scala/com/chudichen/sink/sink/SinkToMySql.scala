package com.chudichen.sink.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.chudichen.sink.model.Student
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * @author chudichen
 * @since 2020-11-23
 */
class SinkToMySql extends RichSinkFunction[Student] {

  private var ps: PreparedStatement = _
  private var connection: Connection = _

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    connection = getConnection
    val sql = "insert into student(id, name, password, age) values (?,?,?,?)"
    if (connection != null) {
      ps = this.connection.prepareStatement(sql)
    }
  }

  override def close(): Unit = {
    super.close()
    if (connection != null) {
      connection.close()
    }
    if (ps != null) {
      ps.close()
    }
  }

  override def invoke(value: Student, context: SinkFunction.Context[_]): Unit = {
    if (ps == null) {
      return
    }

    ps.setInt(1, value.id)
    ps.setString(2, value.name)
    ps.setString(3, value.password)
    ps.setInt(4, value.age)
    ps.executeUpdate()
  }

  private def getConnection: Connection = {
    var connection: Connection = null
    try {
      Class.forName("com.mysql.cj.jdbc.Driver")
      connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useUnicode=true&characterEncoding=UTF-8", "root", "root");
    } catch {
      case e: Exception => e.printStackTrace()
    }
    connection
  }
}
