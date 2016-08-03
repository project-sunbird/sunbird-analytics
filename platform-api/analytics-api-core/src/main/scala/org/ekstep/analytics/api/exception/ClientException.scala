package org.ekstep.analytics.api.exception

case class ClientException(msg: String, ex: Exception = null) extends Exception(msg, ex)  {
  
}