package com.chudichen.datasource.util

import java.sql.{Connection, DriverManager}

/**
 * Mysql工具类
 *
 * @author chudichen
 * @since 2020-11-19
 */
object MysqlUtil {

  /**
   * 获取链接
   *
   * @param driver 驱动
   * @param url 链接地址
   * @param user 用户名
   * @param password 密码
   * @return 链接
   */
  def getConnection(driver: String, url: String, user: String, password: String): Connection = {
    try {
      Class.forName(driver)
      DriverManager.getConnection(url, user, password)
    } catch {
      case e: Exception =>
        e.printStackTrace()
        null
    }
  }
}
