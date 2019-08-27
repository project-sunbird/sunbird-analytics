package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.JSONUtils

object AuditComputationModel extends IBatchModelTemplate[Empty, Empty, AuditOutput, AuditOutput] {

  implicit val className: String = "org.ekstep.analytics.model.AuditComputationModel"

  /**
    * Pre processing steps before running the algorithm. Few pre-process steps are
    * 1. Transforming input - Filter/Map etc.
    * 2. Join/fetch data from LP
    * 3. Join/Fetch data from Cassandra
    */
  override def preProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {
    events
  }

  /**
    * Method which runs the actual algorithm
    */
  override def algorithm(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AuditOutput] = {
    val auditConfigString = config.getOrElse("auditRules", "[]").asInstanceOf[String]
    val auditConfigurations = JSONUtils.deserialize[List[AuditConfig]](auditConfigString)
    val auditOutput = auditConfigurations.map {
      auditConfig =>
        AuditTaskRunner.execute(auditConfig)
    }

    sc.parallelize(auditOutput)
  }

  /**
    * Post processing on the algorithm output. Some of the post processing steps are
    * 1. Saving data to Cassandra
    * 2. Converting to "MeasuredEvent" to be able to dispatch to Kafka or any output dispatcher
    * 3. Transform into a structure that can be input to another data product
    */
  override def postProcess(events: RDD[AuditOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AuditOutput] = {
    //events.foreach(println(_))
    val dispatcher = Dispatcher("slack", Map("channel" -> AppConf.getConfig("pipeline_audit.notification.channel"), "userName" -> AppConf.getConfig("pipeline_audit.notification.name"), "attachments" -> "true"))
    val auditOutputList = events.collect().toList
    val colorMap = Map[String, String]("GREEN" -> "good", "RED" -> "danger", "AMBER" -> "warning")
    val messageList = auditOutputList.zipWithIndex.map{ case (audit, index) =>
      s"""
         |{
         | "fallback": "",
         | "pretext": "*Daily Audit Report*",
         | "title": "${index+1}. Job:  ${audit.name}",
         | "title_link": "",
         | "text": "----------",
         | "fields": ${audit.details.flatMap(details => details.toSlackFields).mkString("[", ",", "]")},
         | "color": "${colorMap(audit.status.toString)}"
         |}
      """.stripMargin
        .replaceAll("\\n", "")
    }.toArray

    OutputDispatcher.dispatch(dispatcher, messageList)
    events
  }

}