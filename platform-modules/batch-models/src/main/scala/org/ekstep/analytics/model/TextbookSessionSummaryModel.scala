package org.ekstep.analytics.model

import scala.collection.mutable.Buffer

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.creation.model.CreationEvent
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.Output
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.JobContext
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.Period
/**
 * @author yuva
 */
case class UnitSummary(total_units_added: Long, total_units_deleted: Long, total_units_modified: Long) extends Serializable
case class SubUnitSummary(total_sub_units_added: Long, total_sub_units_deletd: Long, total_sub_units_modified: Long, total_lessons_added: Long, total_lessons_deleted: Long, total_lessons_modified: Long)
case class TextbookSessionMetrics(period: Int, uid: String, sid: String, content_id: String, start_time: Long, end_time: Long, time_spent: Double, time_diff: Double, unit_summary: UnitSummary, sub_unit_summary: SubUnitSummary, date_range: DtRange, syncts: Long) extends Output with AlgoOutput
case class Sessions(period: Int, creationEvent: Buffer[Buffer[CreationEvent]]) extends AlgoInput
case class Sessions1(period: Int, sessionEvents: Buffer[CreationEvent]) extends AlgoInput

/**
 * @dataproduct
 * @Summarizer
 *
 * TextbookSessionSummaryModel
 *
 * Functionality
 * Compute session wise Textbook summary : Units,sub units and lessons added/deleted/modified
 */

object TextbookSessionSummaryModel extends IBatchModelTemplate[CreationEvent, Sessions, TextbookSessionMetrics, MeasuredEvent] with Serializable {
    implicit val className = "org.ekstep.analytics.model.TextbookSessionSummaryModel"
    override def name(): String = "TextbookSessionSummaryModel";
    override def preProcess(data: RDD[CreationEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Sessions] = {
        /*
         * Input raw telemetry
         * */
        val filteredData = data.filter { x => (x.edata.eks.env != null) }.collect().toBuffer
        val sortedEvent = filteredData.sortBy { x => x.ets }
        val sortedEventRDD = sc.parallelize(sortedEvent)
        sortedEventRDD.map { f =>
            val period = CommonUtil.getPeriod(f.ets, Period.DAY);
            (period, Buffer(f))
        }.partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => Sessions1(x._1, x._2) }.map { x => Sessions(x.period, getSessions(x.sessionEvents)) }

    }

    override def algorithm(data: RDD[Sessions], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[TextbookSessionMetrics] = {
        val idleTime = config.getOrElse("idleTime", 600).asInstanceOf[Int];

        data.map { x =>
            val period = x.period
            x.creationEvent.map { x =>

                val start_time = x.head.ets
                val end_time = x.last.ets
                val date_range = DtRange(start_time, end_time)
                var tmpLastEvent: CreationEvent = null;
                val eventsWithTs = x.map { x =>
                    if (tmpLastEvent == null) tmpLastEvent = x;
                    val ts = CommonUtil.getTimeDiff(tmpLastEvent.ets, x.ets).get;
                    tmpLastEvent = x;
                    (x, if (ts > idleTime) 0 else ts)
                }
                val time_spent = CommonUtil.roundDouble(eventsWithTs.map(f => f._2).sum, 2);
                val time_diff = CommonUtil.roundDouble(CommonUtil.getTimeDiff(start_time, end_time).get, 2);
                val uid = x.head.uid
                val sid = x.head.context.get.sid
                val content_id = x.head.context.get.content_id
                val total_units_added = x.filter { x => (x.edata.eks.target.equals("") && x.edata.eks.targetid.equals("add_unit")) }.size
                val total_units_deleted = x.filter { x => (x.edata.eks.target.equals("textbookunit") && x.edata.eks.subtype.equals("delete")) }.size
                //to-do
                val total_units_modified = 0L
                //to-do
                val total_sub_units_deleted = 0L
                //to-do
                val total_sub_units_modified = 0L
                //to-do
                val total_lessons_modified = 0L
                val total_sub_units_added = x.filter { x => (x.edata.eks.target.equals("") && x.edata.eks.targetid.equals("add_sub_unit")) }.size
                val total_lessons_added = x.filter { x => (x.edata.eks.target.equals("textbookunit") && x.edata.eks.targetid.equals("add_lesson") && x.edata.eks.subtype.equals("change")) }.size
                val total_lessons_deleted = x.filter { x => (x.edata.eks.target.equals("textbookunit") && x.edata.eks.subtype.equals("delete")) }.size
                TextbookSessionMetrics(period, uid, sid, content_id, start_time, end_time, time_spent, time_diff, UnitSummary(total_units_added, total_units_deleted, total_units_modified), SubUnitSummary(total_sub_units_added, total_sub_units_deleted, total_sub_units_modified, total_lessons_added, total_lessons_deleted, total_lessons_modified), date_range,end_time)
            }

        }.flatMap { x => x }
    }

    override def postProcess(data: RDD[TextbookSessionMetrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { summary =>
            val mid = CommonUtil.getMessageId("ME_TEXTBOOK_SESSION_SUMMARY", summary.sid + summary.content_id + summary.uid, config.getOrElse("granularity", "DAY").asInstanceOf[String], summary.start_time);
            val measures = Map(
                "start_time" -> summary.start_time,
                "end_time" -> summary.end_time,
                "time_spent" -> summary.time_spent,
                "time_diff" -> summary.time_diff,
                "unit_summary" -> summary.unit_summary,
                "sub_unit_summary" -> summary.sub_unit_summary,
                "date_range" -> summary.date_range);
            val pdata = PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "TextbookSessionSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]);
            MeasuredEvent("ME_TEXTBOOK_SESSION_SUMMARY", System.currentTimeMillis(), summary.syncts, "1.0", mid, summary.uid, None, None,
                Context(pdata, None, "DAY", summary.date_range),
                Dimensions(Option(summary.uid), None, None, None, None, None, None, None, None, None, Option(summary.period), Option(summary.content_id), None, None, Option(summary.sid), None), MEEdata(measures), None);
        };
    }

    /*
     * Sessionization based on Env
     * */
    private def getSessions(creationEvent: Buffer[CreationEvent]): Buffer[Buffer[CreationEvent]] = {
        var sessions = Buffer[Buffer[CreationEvent]]();
        var tmpArr = Buffer[CreationEvent]();
        var prevEnv = ""
        creationEvent.foreach { x =>
            x.edata.eks.env match {
                case "textbook" => if ((prevEnv.equals("textbook") && prevEnv.equals(x.edata.eks.env)) && (CommonUtil.getTimeDiff(tmpArr.last.ets, x.ets).get / 60 < 30)) {
                    tmpArr += x
                } else {
                    if (tmpArr.length > 0)
                        sessions += tmpArr
                    tmpArr = Buffer[CreationEvent]();
                    tmpArr += x
                }
                case _ =>
            }
            prevEnv = x.edata.eks.env
        }
        sessions
    }
}