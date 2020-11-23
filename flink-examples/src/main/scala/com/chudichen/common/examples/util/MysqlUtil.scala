package com.chudichen.common.examples.util

import java.sql.{Connection, DriverManager}

/**
 * @author chudichen
 * @since 2020-11-20
 */
object MysqlUtil {

  def getConnection(driver: String, url: String, user: String, password: String): Connection = {
    try {
      Class.forName(driver)
      DriverManager.getConnection(url, user, password)
    } catch {
      case e: Exception => e.printStackTrace()
        null
    }
  }
}
