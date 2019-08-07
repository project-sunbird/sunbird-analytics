package org.ekstep.analytics.api.service

import akka.actor.Actor
import org.ekstep.analytics.api.util.APILogger
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.HTTPClient

class DruidHealthCheckService(restUtil: HTTPClient) extends Actor {

  implicit val className = "org.ekstep.analytics.api.service.DruidHealthCheckService"
  val apiUrl = AppConf.getConfig("druid.coordinator.host")+AppConf.getConfig("druid.healthcheck.url")

  def receive = {
    case _ => sender() ! getStatus
  }

  def getStatus: String = {
    val healthreport: StringBuilder = new StringBuilder()
    try{
      val response = restUtil.get[Map[String, Double]](apiUrl)
      response.map { data =>
        healthreport.append("http_druid_health_check_status{datasource=\"")
          .append(data._1).append("\"} ")
          .append(data._2).append("\n")
      }
      healthreport.toString()
    }
    catch {
      case ex: Exception =>
        ex.printStackTrace()
        APILogger.log("",Some("DruidHealthCheckAPI failed due to " + ex.getMessage), "DruidHealthCheck")
        healthreport.toString()
    }
  }
}
