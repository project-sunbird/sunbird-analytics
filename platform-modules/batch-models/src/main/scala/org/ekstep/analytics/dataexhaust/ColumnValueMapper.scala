package org.ekstep.analytics.dataexhaust

import org.joda.time.DateTime

object ColumnValueMapper {
  def mapValue(funcName: String, arg: String): String = {
    try {
      funcName match {
        case "timestampToDateTime" => timestampToDateTime(arg)
        case _                     => arg
      }
    } catch {
      // There is no point in handling exception here. Hence ignoring it and returning the original arg
      case e: Exception => arg
    }
  }

  def timestampToDateTime(value: String): String = {
    new DateTime(value.toLong).toString()
  }
}