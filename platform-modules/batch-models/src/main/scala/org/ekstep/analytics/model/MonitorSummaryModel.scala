package org.ekstep.analytics.model

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.creation.model.CreationEData
import org.ekstep.analytics.creation.model.CreationEks
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants
import com.flyberrycapital.slack.SlackClient
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import net.liftweb.json.DefaultFormats
import org.ekstep.analytics.framework.util.JSONUtils

/**
 * @dataproduct
 * @Summarizer
 *
 * MonitorSummaryModel
 *
 * Functionality
 * 1. Monitor all data products. This would be used to keep track of all data products.
 * Events used - BE_JOB_*
 */
case class JobMonitor(jobs_started: Long, jobs_completed: Long, jobs_failed: Long, total_events_generated: Long, total_ts: Double, syncTs: Long, job_summary: Array[Map[String, Any]], dtange: DtRange) extends AlgoOutput
case class JobMonitorToSlack(jobs_started: Long, jobs_completed: Long, jobs_failed: Long, total_events_generated: Long, total_ts: Double, job_summary: Array[Map[String, Any]]) extends AlgoOutput

object MonitorSummaryModel extends IBatchModelTemplate[DerivedEvent, DerivedEvent, JobMonitor, MeasuredEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.MonitorSummaryModel"
    override def name: String = "MonitorSummaryModel"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        
      val filtered_data =  data.filter { x => (x.eid.equals("BE_JOB_START") || (x.eid.equals("BE_JOB_END"))) }
        filtered_data.sortBy(_.ets)
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[JobMonitor] = {
        val jobs_started = data.filter { x => (x.eid.equals("BE_JOB_START")) }.count()
        val eks_map = data.map { x => (x.edata.eks.asInstanceOf[Map[String, String]]) }
        val jobs_completed = eks_map.filter { x => (x.get("status").getOrElse("").equals("SUCCESS")) }.count()
        val jobs_failed = eks_map.filter { x => (x.get("status").getOrElse("").equals("FAILED")) }.count()
        val jobs_end = data.filter { x => (x.eid.equals("BE_JOB_END")) }
        val syncTs = data.first().syncts
        val total_events_generated = jobs_end.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("data", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("outputEvents", 0L).asInstanceOf[Number].longValue() }.sum().longValue()
        val total_ts = jobs_end.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("data", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("timeTaken", 0.0).asInstanceOf[Number].doubleValue() }.sum()
        val job_summary = jobs_end.map { x =>
            val model = x.context.pdata.model
            val input_count = x.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("data", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("inputEvents", 0L).asInstanceOf[Number].longValue()
            val output_count = x.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("data", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("outputEvents", 0L).asInstanceOf[Number].longValue()
            val time_taken = x.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("data", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("timeTaken", 0.0).asInstanceOf[Number].floatValue()
            val status = x.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("status", "").toString()
            val err_message = x.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("message", "").toString()
            val day = CommonUtil.getPeriod(x.ets, DAY)
            Map("model" -> model, "input_count" -> input_count, "output_count" -> output_count, "time_taken" -> time_taken, "status" -> status, "day" -> day)
        }.collect()
        sc.parallelize(List(JobMonitor(jobs_started, jobs_completed, jobs_failed, total_events_generated, total_ts, syncTs, job_summary, DtRange(data.first().ets, data.collect().last.ets))))
    }

    override def postProcess(data: RDD[JobMonitor], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        /*implicit val formats = DefaultFormats
        val job_monitor_to_sclack = data.map { x => JobMonitorToSlack(x.jobs_started, x.jobs_failed, x.jobs_completed, x.total_events_generated, x.total_ts, x.job_summary) }.first()
       
        val token = new SlackClient("xoxp-19320441927-104869600647-206793219988-59016df1fed2d44716d0dc1ea26f6d01")
        val map_values = job_monitor_to_sclack.job_summary.map(f => f.values)
        val header = "`Model `," + "`Input Events `," + "`Output Events `," + "`Total time `," + "`Status `," + "`day`"
        val json_string = "`jobs started :`" + job_monitor_to_sclack.jobs_started + "\n" + "`jobs_comleted` :" + job_monitor_to_sclack.jobs_completed + "\n" + "`jobs_failed` :" + job_monitor_to_sclack.jobs_failed + "\n" + "`total_events_generated :`" + job_monitor_to_sclack.total_events_generated + "\n" + "`total time taken :`" + job_monitor_to_sclack.total_ts + "\n" + "`jobs_summary :`\n" + header + "\n" + JSONUtils.serialize(map_values) + "\n"
        token.chat.postMessage("testing", json_string)*/
        
        data.map { x =>
            val mid = CommonUtil.getMessageId("ME_MONITOR_SUMMARY", "", "DAY", x.dtange);
            val measures = Map(
                "jobs_start_count" -> x.jobs_started,
                "jobs_completed_count" -> x.jobs_completed,
                "jobs_failed_count" -> x.jobs_failed,
                "total_events_generated" -> x.total_events_generated,
                "total_ts" -> x.total_ts,
                "jobs_summary" -> x.job_summary);

            MeasuredEvent("ME_MONITOR_SUMMARY", System.currentTimeMillis(), x.syncTs, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "MonitorSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "DAY", x.dtange),
                Dimensions(None, None, None, None, None, None, None, None, None, None, Option(CommonUtil.getPeriod(x.syncTs, DAY)), None, None, None, None, None, None, None, None, None, None, None, None, None),
                MEEdata(measures), None);
        }
    }

}