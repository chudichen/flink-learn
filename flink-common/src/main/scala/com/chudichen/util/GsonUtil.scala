package com.chudichen.util

import java.lang.reflect.Type
import java.nio.charset.Charset

import com.google.gson.{Gson, GsonBuilder}

/**
 * json工具类
 *
 * @author chudichen
 * @since 2020-11-19
 */
object GsonUtil {

  private val gson = new Gson()
  private val disableHtmlEscapingGson = new GsonBuilder().disableHtmlEscaping().create()

  def fromJson[T](value: String, clazz: Class[T]):T = {
    gson.fromJson(value, clazz)
  }

  def fromJson[T](string: String, t:Type):T = {
    gson.fromJson(string, t)
  }

  def toJson(value: Any): String = {
    gson.toJson(value)
  }

  def toJsonDisableHtmlEscaping(value: Any):String = {
    disableHtmlEscapingGson.toJson(value)
  }

  def toJsonBytes(value: Any) = {
    gson.toJson(value).getBytes(Charset.forName("UTF-8"))
  }
}
