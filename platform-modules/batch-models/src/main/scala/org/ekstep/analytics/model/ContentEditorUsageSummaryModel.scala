/**
 * @author Jitendra Singh Sankhwar
 */
package org.ekstep.analytics.model

import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import scala.collection.mutable.ListBuffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.util.CommonUtil

/**
 * Case class to hold the usage summary input and output
 */
case class CEUsageInput(period: Int, sessionEvents: Buffer[DerivedEvent]) extends AlgoInput
case class CEUsageMetricsSummary(period: Int, content_id: String, dtRange: DtRange, unique_users_count: Long, total_sessions: Long, total_ts: Double, avg_ts_session: Double, syncts: Long) extends AlgoOutput;

/**
 * @dataproduct
 * @Summarizer
 *
 * ContentEditorUsageSummaryModel
 *
 * Functionality
 * 1. Generate content editor usage summary events per day. This would be used to compute content editor usage weekly, monthly & cumulative metrics.
 * Event used - ME_CE_SESSION_SUMMARY
 */
object ContentEditorUsageSummaryModel extends IBatchModelTemplate[DerivedEvent, CEUsageInput, CEUsageMetricsSummary, MeasuredEvent] with Serializable {
    
    val className = "org.ekstep.analytics.model.ContentEditorUsageSummaryModel"
    override def name: String = "ContentEditorUsageSummaryModel"
    
    /**
     * To calculate the usage summary per content_id
     * @param data is RDD of session events
     * @return the output RDD of CEUsageMetricsSummary per content
     */
    def getContentSpecificSummary(data: RDD[CEUsageInput]): RDD[CEUsageMetricsSummary] = {
        data.map { event =>
            val filteredSessions = event.sessionEvents.filter { x => !x.content_id.get.isEmpty }
            filteredSessions.groupBy { x => x.content_id }.map { f =>
                val firstEvent = f._2.sortBy { x => x.context.date_range.from }.head
                val lastEvent = f._2.sortBy { x => x.context.date_range.to }.last
                val date_range = DtRange(firstEvent.context.date_range.from, lastEvent.context.date_range.to);
                val userCount = f._2.map(x => x.uid).distinct.filterNot { x => x.isEmpty() }.toList.length.toLong;
                val eksMapList = f._2.map { x =>
                    x.edata.eks.asInstanceOf[Map[String, AnyRef]]
                }
                val totalSessions = f._2.length.toLong
                val totalTS = eksMapList.map { x =>
                    x.get("time_spent").get.asInstanceOf[Double]
                }.sum
                val avgSessionTS = if (totalTS == 0 || totalSessions == 0) 0d else BigDecimal(totalTS / totalSessions).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;
                CEUsageMetricsSummary(event.period, f._1.get, date_range, userCount, totalSessions, totalTS, avgSessionTS, lastEvent.syncts);
            }
        }.flatMap { x => x }
    }
    
    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CEUsageInput] = {
        data.map { f =>
            val period = CommonUtil.getPeriod(f.context.date_range.to, Period.DAY);
            (period, Buffer(f))
        }.partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => CEUsageInput(x._1, x._2) };
    }

    override def algorithm(data: RDD[CEUsageInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CEUsageMetricsSummary] = {
        val contentSpecificUsage = getContentSpecificSummary(data)
        data.map { f =>
            val firstEvent = f.sessionEvents.sortBy { x => x.context.date_range.from }.head
            val lastEvent = f.sessionEvents.sortBy { x => x.context.date_range.to }.last
            val date_range = DtRange(firstEvent.context.date_range.from, lastEvent.context.date_range.to);

            val eksMapList = f.sessionEvents.map { x =>
                x.edata.eks.asInstanceOf[Map[String, AnyRef]]
            }
            val userCount = f.sessionEvents.map(x => x.uid).distinct.filterNot { x => x.isEmpty() }.toList.length.toLong
            val totalSessions = f.sessionEvents.length.toLong
            val totalTS = eksMapList.map { x =>
                x.get("time_spent").get.asInstanceOf[Double]
            }.sum

            val avgSessionTS = if (totalTS == 0 || totalSessions == 0) 0d else BigDecimal(totalTS / totalSessions).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble;
            
            CEUsageMetricsSummary(f.period, "all", date_range, userCount, totalSessions, totalTS, avgSessionTS, lastEvent.syncts)
        } ++ contentSpecificUsage
    }

    override def postProcess(data: RDD[CEUsageMetricsSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { usageSumm =>
            val mid = CommonUtil.getMessageId("ME_CE_USAGE_SUMMARY", usageSumm.content_id, "DAY", usageSumm.dtRange);
            val measures = Map(
                "unique_users_count" -> usageSumm.unique_users_count,
                "total_sessions" -> usageSumm.total_sessions,
                "total_ts" -> usageSumm.total_ts,
                "avg_ts_session" -> usageSumm.avg_ts_session);
            MeasuredEvent("ME_CE_USAGE_SUMMARY", System.currentTimeMillis(), usageSumm.syncts, "1.0", mid, "", None, None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "ContentEditorUsageSummarizer").asInstanceOf[String])), None, "DAY", usageSumm.dtRange),
                Dimensions(None, None, None, None, None, None, None, None, None, None, None, Option(usageSumm.period), Option(usageSumm.content_id)),
                MEEdata(measures), None);
        }
    }
}