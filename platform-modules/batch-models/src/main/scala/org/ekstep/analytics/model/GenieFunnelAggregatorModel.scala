package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.DataFilter
import scala.collection.mutable.Buffer
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.MEEdata
import scala.collection.mutable.HashMap
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.Period
import org.ekstep.analytics.framework.Stage
import org.ekstep.analytics.framework.OnboardStage
import org.ekstep.analytics.framework.OtherStage
import org.ekstep.analytics.framework.util.JSONUtils

case class DeviceFunnelSummary(did: String, funnel: String, events: Buffer[DerivedEvent]) extends AlgoInput
case class FunnelSummary(funnel: String, did: String, period: Int, dspec: Map[String, AnyRef], genieVer: String, summary: Map[String, StageAggSumm], totalCount: Int, totalTimeSpent: Double, avgTimeSpent: Double, syncts: Long, dateRange: DtRange, tags: Option[AnyRef]) extends AlgoOutput
case class StageAggSumm(label: String, totalCount: Int, totalInvocations: Int, completionPercentage: Double, dropoffPercentage: Double)

object GenieFunnelAggregatorModel extends IBatchModelTemplate[DerivedEvent, DeviceFunnelSummary, FunnelSummary, MeasuredEvent] with Serializable {

	val className = "org.ekstep.analytics.model.GenieFunnelAggregatorModel"
	override def name: String = "GenieFunnelAggregatorModel"
	
    def _funnelAggregator(data: DeviceFunnelSummary): FunnelSummary = {
        val funnel = data.funnel
        if ("GenieOnboarding".equals(funnel)) {
            _getFunnelSummary(data, OnboardStage)
        } else {
            _getFunnelSummary(data, OtherStage)
        }
    }

    private def _getFunnelSummary(data: DeviceFunnelSummary, stage: Stage): FunnelSummary = {

        val events = data.events.sortBy { x => x.ets }
        val eventCount = events.size
        val firstEvent = events.head
        val lastEvent = events.last
        val dimensions = firstEvent.dimensions

        val eksMaps = data.events.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]] }
        val totalTimeSpent = CommonUtil.roundDouble(eksMaps.map { x => (x.get("timeSpent").get.asInstanceOf[Double]) }.sum, 2)
        val period = CommonUtil.getPeriod(firstEvent.syncts, Period.DAY)

        val avgTimeSpent = if (0 != eventCount) CommonUtil.roundDouble(totalTimeSpent / eventCount, 2) else 0d

        val stageSumm = _getStageSummMap(eksMaps, stage)
        FunnelSummary(data.funnel, data.did, period, dimensions.dspec.getOrElse(Map()), dimensions.genieVer.get, stageSumm, eventCount, totalTimeSpent, avgTimeSpent, firstEvent.syncts, DtRange(firstEvent.context.date_range.from, lastEvent.context.date_range.to), firstEvent.tags)
    }

    private def _getStageSummMap(eksMaps: Buffer[Map[String, AnyRef]], stage: Stage): Map[String, StageAggSumm] = {
        stage.values.map { x =>
            val stageId = x.toString()
            (stageId, eksMaps.map { x => (x.get(stageId).get.asInstanceOf[Map[String, AnyRef]]) })
        }.toMap.map { x => (x._1, _getStageAggSumm(x._2, x._1)) }

    }
    private def _getStageAggSumm(data: Buffer[Map[String, AnyRef]], label: String): StageAggSumm = {
        val stageSumm = data.map { x => FunnelStageSummary(label, Option(x.get("count").get.asInstanceOf[Int]), Option(x.get("stageInvoked").get.asInstanceOf[Int])) }
        val totalCount = stageSumm.map { x => x.count.get }.sum
        val stageInvokedCount = stageSumm.map { x => x.stageInvoked.get }.filter { x => x != 0 }.size
        val alleventSize = data.size
        val dropoffPercentage: Double = CommonUtil.roundDouble(((alleventSize - stageInvokedCount.toDouble) / alleventSize) * 100, 2)
        StageAggSumm(label, totalCount, stageInvokedCount, CommonUtil.roundDouble(100 - dropoffPercentage, 2), dropoffPercentage);
    }
    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DeviceFunnelSummary] = {
        val events = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_GENIE_FUNNEL")));
        val didEvent = events.map { x => (x.dimensions.did.get, Buffer(x)) }
        val didFunnelSumm = didEvent.partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b)

        didFunnelSumm.map { x =>
            val did = x._1
            val data = x._2.map { x => (x.dimensions.funnel.get, x) }
            data.groupBy { x => x._1 }.map { x => DeviceFunnelSummary(did, x._1, x._2.map { x => x._2 }) }
        }.flatMap { x => x }
    }

    override def algorithm(data: RDD[DeviceFunnelSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[FunnelSummary] = {
        data.map { x =>
            _funnelAggregator(x)
        }
    }

    override def postProcess(data: RDD[FunnelSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { summary =>
            val mid = CommonUtil.getMessageId("ME_GENIE_FUNNEL_USAGE_SUMMARY", summary.funnel, config.getOrElse("granularity", "FUNNEL").asInstanceOf[String], summary.dateRange, summary.did);
            val measures = summary.summary.toMap ++ Map("totalTimeSpent" -> summary.totalTimeSpent, "totalCount" -> summary.totalCount, "avgTimeSpent" -> summary.avgTimeSpent) //, "completionPercentage" -> summary.completionPercentage)
            MeasuredEvent("ME_GENIE_FUNNEL_USAGE_SUMMARY", System.currentTimeMillis(), summary.syncts, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "GenieFunnelAggregator").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "FUNNEL").asInstanceOf[String], summary.dateRange),
                Dimensions(None, Option(summary.did), None, None, None, None, None, None, None, None, Option(summary.period), None, None, None, None, None, Option(summary.funnel), Option(summary.dspec), None, Option(summary.genieVer)),
                MEEdata(measures), summary.tags);
        }
    }
}