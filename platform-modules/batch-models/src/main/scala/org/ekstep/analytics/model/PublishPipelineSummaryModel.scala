package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.util.SessionBatchModel
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.AlgoOutput
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.util._
import org.ekstep.analytics.framework._
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.joda.time.format.DateTimeFormat
import java.util.Formatter.DateTime
import java.util.Formatter.DateTime
import java.util.Date
import java.text.SimpleDateFormat
import org.ekstep.analytics.creation.model.CreationEvent
import org.ekstep.analytics.util.CreationEventUtil
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.creation.model.CreationPData

case class EventsByPeriod(period: Long, channel: String, events: Buffer[CreationEvent]) extends AlgoInput
case class PublishPipelineSummary(`type`: String, state: String, subtype: String, count: Int)
case class PipelineSummaryOutput(summary: Iterable[PublishPipelineSummary], date_range: DtRange, period: Long, channel: String, pdata: CreationPData, syncts: Long) extends AlgoOutput

object PublishPipelineSummaryModel extends IBatchModelTemplate[CreationEvent, EventsByPeriod, PipelineSummaryOutput, MeasuredEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.PublishPipelineSummaryModel"
    override def name: String = "PublishPipelineSummaryModel"

    override def preProcess(data: RDD[CreationEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[EventsByPeriod] = {
        JobLogger.log("Filtering Events of BE_OBJECT_LIFECYCLE")
        val objectLifecycleEvents = DataFilter.filter(data, Array(Filter("eventId", "IN", Option(List("BE_OBJECT_LIFECYCLE")))));
        objectLifecycleEvents.sortBy { x => x.ets }.map { x =>
            val period = CommonUtil.getPeriod(x.ets, Period.DAY)
            val channel = CreationEventUtil.getChannelId(x)
            ((period, channel), Buffer(x))
        }.partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b)
            .map(x => EventsByPeriod(x._1._1, x._1._2, x._2))
    }

    override def algorithm(input: RDD[EventsByPeriod], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PipelineSummaryOutput] = {

        val data = input.cache()
        val summaries = data.map { d =>
            val groups = d.events.groupBy(e => (e.edata.eks.`type`, e.edata.eks.state, e.edata.eks.subtype))
            val summaryOutput = groups.map { e =>
                val _type = e._1._1
                val state = e._1._2
                val subtype = e._1._3
                val count = e._2.length
                PublishPipelineSummary(_type, state, subtype, count)
            }

            val dateRange = DtRange(d.events.head.ets, d.events.last.ets)
            val pdata = CreationEventUtil.getAppDetails(d.events.head)
            PipelineSummaryOutput(summaryOutput, dateRange, d.period, d.channel, pdata, CreationEventUtil.getEventSyncTS(d.events.last))
        }

        data.unpersist(true)

        summaries
    }

    override def postProcess(data: RDD[PipelineSummaryOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { x =>
            val measures = Map(
                "publish_pipeline_summary" -> x.summary)
            val mid = CommonUtil.getMessageId("ME_PUBLISH_PIPELINE_SUMMARY", x.period.toString(), "DAY", x.date_range, "", Option(x.pdata.id), Option(x.channel));
            MeasuredEvent("ME_PUBLISH_PIPELINE_SUMMARY", System.currentTimeMillis(), x.syncts, "1.0", mid, "", Option(x.channel), None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "PublishPipelineSummarizer").asInstanceOf[String])), None, "DAY", x.date_range),
                Dimensions(None, None, None, None, None, None, Option(PData(x.pdata.id, x.pdata.ver)), None, None, None, None, Option(x.period.toInt)),
                MEEdata(measures), None);
        }
    }
}
