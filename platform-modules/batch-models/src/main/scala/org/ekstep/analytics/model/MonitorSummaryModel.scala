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
import org.ekstep.analytics.framework.conf.AppConf

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
case class JobSummary(model: String, input_count: Long, output_count: Long, time_taken: Double, status: String, day: Int) extends AlgoOutput

object MonitorSummaryModel extends IBatchModelTemplate[DerivedEvent, DerivedEvent, JobMonitor, MeasuredEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.MonitorSummaryModel"
    override def name: String = "MonitorSummaryModel"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        
        val filteredData = data.filter { x => (x.eid.equals("BE_JOB_START") || (x.eid.equals("BE_JOB_END"))) }
        filteredData.sortBy(_.ets)
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[JobMonitor] = {
        val jobsStarted = data.filter { x => (x.eid.equals("BE_JOB_START")) }.count()
        val filteresData = data.filter { x => (x.eid.equals("BE_JOB_END")) }
        val eksMap = filteresData.map { x => (x.edata.eks.asInstanceOf[Map[String, String]]) }
        val jobsCompleted = eksMap.filter { x => (x.get("status").getOrElse("").equals("SUCCESS")) }.count()
        val jobsFailed = eksMap.filter { x => (x.get("status").getOrElse("").equals("FAILED")) }.count()
        val jobsEnd = data.filter { x => (x.eid.equals("BE_JOB_END")) }
        val syncTs = data.first().syncts
        val totalEventsGenerated = jobsEnd.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("data", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("outputEvents", 0L).asInstanceOf[Number].longValue() }.sum().longValue()
        val totalTs = jobsEnd.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("data", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("timeTaken", 0.0).asInstanceOf[Number].doubleValue() }.sum()
        val jobSummary = jobsEnd.map { x =>
            val model = x.context.pdata.model
            val inputCount = x.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("data", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("inputEvents", 0L).asInstanceOf[Number].longValue()
            val outputCount = x.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("data", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("outputEvents", 0L).asInstanceOf[Number].longValue()
            val timeTaken = x.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("data", Map()).asInstanceOf[Map[String, AnyRef]].getOrElse("timeTaken", 0.0).asInstanceOf[Number].floatValue()
            val status = x.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("status", "").toString()
            val errMessage = x.edata.eks.asInstanceOf[Map[String, AnyRef]].getOrElse("message", "").toString()
            val day = CommonUtil.getPeriod(x.ets, DAY)
            Map("model" -> model, "input_count" -> inputCount, "output_count" -> outputCount, "time_taken" -> timeTaken, "status" -> status, "day" -> day)
        }.collect()
        sc.parallelize(List(JobMonitor(jobsStarted, jobsCompleted, jobsFailed, totalEventsGenerated, totalTs, syncTs, jobSummary, DtRange(data.first().ets, data.collect().last.ets))))
    }

    override def postProcess(data: RDD[JobMonitor], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        val message = messageFormatToSlack(data.first())
        if ("true".equalsIgnoreCase(AppConf.getConfig("monitor.notification.slack"))) {
            val token = new SlackClient(AppConf.getConfig("monitor.notification.token"))
            token.chat.postMessage(AppConf.getConfig("monitor.notification.channel"), message)
        } else {
            println(message)

        }
        
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
                Context(PData(config.getOrElse("id", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "MonitorSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, "DAY", x.dtange),
                Dimensions(None, None, None, None, None, None, None, None, None, None, Option(CommonUtil.getPeriod(x.syncTs, DAY)), None, None, None, None, None, None, None, None, None, None, None, None, None),
                MEEdata(measures), None);
        }
    }

    private def messageFormatToSlack(jobMonitorToSclack: JobMonitor): String = {

        implicit val formats = DefaultFormats
        val jobsStarted = jobMonitorToSclack.jobs_started
        val jobsCompleted = jobMonitorToSclack.jobs_completed
        val jobsFailed = jobMonitorToSclack.jobs_failed
        val totalEventsGenerated = jobMonitorToSclack.total_events_generated
        val totalTs = CommonUtil.roundDouble(jobMonitorToSclack.total_ts / 3600, 2)
        val jobSummaryCaseClass = jobMonitorToSclack.job_summary.map(f => JobSummary(f.get("model").get.asInstanceOf[String], f.get("input_count").get.asInstanceOf[Number].longValue(), f.get("output_count").get.asInstanceOf[Number].longValue(), f.get("time_taken").get.asInstanceOf[Number].doubleValue(), f.get("status").get.asInstanceOf[String], f.get("day").get.asInstanceOf[Number].intValue()))
        val consumptionModels = jobSummaryCaseClass.filter { x => (x.model.equals("")) }
        val arryString = jobSummaryCaseClass.map { x => x.model.trim() + " ," + x.input_count + " ," + x.output_count + " ," + CommonUtil.roundDouble(x.time_taken / 60, 2) + " ," + x.status + " ," + x.day + "\n" }.mkString("")
        var inputEventMap = collection.mutable.Map[String, Long]()
        var outputEventMap = collection.mutable.Map[String, Long]()
        jobSummaryCaseClass.map { x =>
            inputEventMap += (x.model -> x.input_count)
            outputEventMap += (x.model -> x.output_count)
        }
        val modelMapping = Map("ItemSummaryModel" -> "LearnerSessionSummaryModel",
            "GenieUsageSummaryModel" -> "GenieLaunchSummaryModel",
            "ItemSummaryModel" -> "GenieFunnelModel",
            "GenieStageSummaryModel" -> "GenieLaunchSummaryModel",
            "ItemUsageSummaryModel" -> "ItemSummaryModel",
            "DeviceContentUsageSummaryModel" -> "LearnerSessionSummaryModel",
            "DeviceUsageSummaryModel" -> "GenieLaunchSummaryModel",
            "UpdateGenieUsageDB" -> "GenieUsageSummaryModel",
            "UpdateItemSummaryDB" -> "ItemUsageSummaryModel",
            "UpdateContentPopularityDB" -> "ContentPopularitySummaryModel",
            "UpdateContentUsageDB" -> "ContentUsageSummaryModel",
            "ContentUsageSummaryModel" -> "LearnerSessionSummaryModel")
        var warnings = ""
        modelMapping.map { x =>
            if (outputEventMap(x._2) != inputEventMap(x._1)) {
                val output = x._2
                val input = x._1
                warnings += s"output of $output NOT EQUALS to input of $input\n"
            }
        }

        val header = "Model ," + "Input Events ," + "Output Events ," + "Total time ," + "Status ," + "Day "
        var data = ""
        if (jobsFailed > 0 && warnings.equals("")) {
            data = s"""Number of Jobs Started: `$jobsStarted`\nNumber of Completed Jobs: `$jobsCompleted` \nNumber of failed Jobs: `$jobsFailed` \nTotal time taken: `$totalTs`\nTotal events generated: `$totalEventsGenerated`\n\nDetailed Report:\n```$header \n $arryString```\n\nError: ```"Job Failed"```"""

        } else if (jobsFailed == 0 && warnings.equals("")) {
            data = s"""Number of Jobs Started: `$jobsStarted`\nNumber of Completed Jobs: `$jobsCompleted` \nNumber of failed Jobs: `$jobsFailed` \nTotal time taken: `$totalTs`\nTotal events generated: `$totalEventsGenerated`\n\nDetailed Report:\n```$header \n $arryString```\n\nMessage: ```"Job Run Completed Successfully"```"""

        } else if (jobsFailed == 0 && !warnings.equals("")) {
            data = s"""Number of Jobs Started: `$jobsStarted`\nNumber of Completed Jobs: `$jobsCompleted` \nNumber of failed Jobs: `$jobsFailed` \nTotal time taken: `$totalTs`\nTotal events generated: `$totalEventsGenerated`\n\nDetailed Report:\n```$header \n $arryString```\n\nWarnings: ```$warnings```"""

        } else {
            data = s"""Number of Jobs Started: `$jobsStarted`\nNumber of Completed Jobs: `$jobsCompleted` \nNumber of failed Jobs: `$jobsFailed` \nTotal time taken: `$totalTs`\nTotal events generated: `$totalEventsGenerated`\n\nDetailed Report:\n```$header \n $arryString```\n\nWarnings: ```$warnings```\n\n Error: ```Job Failed```"""

        }
        return data
    }

}