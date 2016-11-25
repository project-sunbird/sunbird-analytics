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

case class DeviceFunnelSummary(did: String, funnel: String, events: Buffer[DerivedEvent]) extends AlgoInput
case class FunnelSummary(funnel: String, did: String, period: Int, dspec: Map[String, AnyRef], genieVer: String, summary: HashMap[String, StageAggSumm], totalTimeSpent: Double, completionPercentage: Double, syncts: Long, dateRange: DtRange, tags: Option[AnyRef]) extends AlgoOutput
case class StageAggSumm(timeSpent: Double, totalCount: Int, dropoffPercentage: Double)

object GenieFunnelAggregatorModel extends IBatchModelTemplate[DerivedEvent, DeviceFunnelSummary, FunnelSummary, MeasuredEvent] with Serializable {

    def _funnelAggregator(data: DeviceFunnelSummary): FunnelSummary = {
        val did = data.did
        val funnel = data.funnel
        val events = data.events.sortBy { x => x.ets }
        val firstEvent = events.head
        val lastEvent = events.last
        val dimensions = firstEvent.dimensions
        val syncts = firstEvent.syncts
        val period = CommonUtil.getPeriod(syncts, Period.DAY)

        val eksMap = data.events.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]] }
        if ("GenieOnboarding".equals(funnel)) {
            val totalTimeSpent = CommonUtil.roundDouble(eksMap.map { x => (x.get("timeSpent").get.asInstanceOf[Double]) }.sum, 2)

            val loadOnboardPage = eksMap.map { x => (x.get("loadOnboardPage").get.asInstanceOf[Map[String, AnyRef]]) }
            val welcomeContentSkipped = eksMap.map { x => (x.get("welcomeContentSkipped").get.asInstanceOf[Map[String, AnyRef]]) }
            val addChildSkipped = eksMap.map { x => (x.get("addChildSkipped").get.asInstanceOf[Map[String, AnyRef]]) }
            val firstLessonSkipped = eksMap.map { x => (x.get("firstLessonSkipped").get.asInstanceOf[Map[String, AnyRef]]) }
            val gotoLibrarySkipped = eksMap.map { x => (x.get("gotoLibrarySkipped").get.asInstanceOf[Map[String, AnyRef]]) }
            val searchLessonSkipped = eksMap.map { x => (x.get("searchLessonSkipped").get.asInstanceOf[Map[String, AnyRef]]) }
            val contentPlayed = eksMap.map { x => (x.get("contentPlayed").get.asInstanceOf[Map[String, AnyRef]]) }

            val contentPlayedSumm = _getStageAggSumm(contentPlayed)
            val completionPercentage = CommonUtil.roundDouble((100 - contentPlayedSumm.dropoffPercentage), 2)

            val stageSumm = HashMap(
                "loadOnboardPage" -> _getStageAggSumm(loadOnboardPage),
                "welcomeContentSkipped" -> _getStageAggSumm(welcomeContentSkipped),
                "addChildSkipped" -> _getStageAggSumm(addChildSkipped),
                "firstLessonSkipped" -> _getStageAggSumm(firstLessonSkipped),
                "gotoLibrarySkipped" -> _getStageAggSumm(gotoLibrarySkipped),
                "searchLessonSkipped" -> _getStageAggSumm(searchLessonSkipped),
                "contentPlayed" -> contentPlayedSumm)

            FunnelSummary(funnel, did, period, dimensions.dspec.getOrElse(Map()), dimensions.genieVer.get, stageSumm, totalTimeSpent, completionPercentage, syncts, DtRange(firstEvent.context.date_range.from, lastEvent.context.date_range.to), firstEvent.tags)
        } else {
            val totalTimeSpent = CommonUtil.roundDouble(eksMap.map { x => (x.get("timeSpent").get.asInstanceOf[Double]) }.sum, 2)

            val listContent = eksMap.map { x => (x.get("listContent").get.asInstanceOf[Map[String, AnyRef]]) }
            val selectContent = eksMap.map { x => (x.get("selectContent").get.asInstanceOf[Map[String, AnyRef]]) }
            val downloadInitiated = eksMap.map { x => (x.get("downloadInitiated").get.asInstanceOf[Map[String, AnyRef]]) }
            val downloadComplete = eksMap.map { x => (x.get("downloadComplete").get.asInstanceOf[Map[String, AnyRef]]) }
            val contentPlayed = eksMap.map { x => (x.get("contentPlayed").get.asInstanceOf[Map[String, AnyRef]]) }

            val contentPlayedSumm = _getStageAggSumm(contentPlayed)
            val completionPercentage = CommonUtil.roundDouble((100 - contentPlayedSumm.dropoffPercentage), 2)

            val stageSumm = HashMap(
                "listContent" -> _getStageAggSumm(listContent),
                "selectContent" -> _getStageAggSumm(selectContent),
                "downloadInitiated" -> _getStageAggSumm(downloadInitiated),
                "downloadComplete" -> _getStageAggSumm(downloadComplete),
                "contentPlayed" -> contentPlayedSumm)

            FunnelSummary(funnel, did, period, dimensions.dspec.getOrElse(Map()), dimensions.genieVer.get, stageSumm, totalTimeSpent, completionPercentage, syncts, DtRange(firstEvent.context.date_range.from, lastEvent.context.date_range.to), firstEvent.tags)
        }
    }

    private def _getStageAggSumm(data: Buffer[Map[String, AnyRef]]): StageAggSumm = {
        val stageSumm = data.map { x => FunnelStageSummary(Option(x.get("timeSpent").get.asInstanceOf[Double]), Option(x.get("count").get.asInstanceOf[Int]), Option(x.get("stageInvoked").get.asInstanceOf[Int])) }
        val timeSpent = CommonUtil.roundDouble(stageSumm.map { x => x.timeSpent.get }.sum, 2)
        val totalCount = stageSumm.map { x => x.count.get }.sum
        val stageInvokedCount = stageSumm.map { x => x.stageInvoked.get }.filter { x => x != 0 }.size
        val alleventSize = data.size
        val dropoffPercentage: Double = CommonUtil.roundDouble(((alleventSize - stageInvokedCount.toDouble) / alleventSize) * 100, 2)
        StageAggSumm(timeSpent, totalCount, dropoffPercentage);
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
            val measures = summary.summary.toMap ++ Map("totalTimeSpent" -> summary.totalTimeSpent, "completionPercentage" -> summary.completionPercentage)
            MeasuredEvent("ME_GENIE_FUNNEL_USAGE_SUMMARY", System.currentTimeMillis(), summary.syncts, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "GenieFunnelAggregator").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "FUNNEL").asInstanceOf[String], summary.dateRange),
                Dimensions(None, Option(summary.did), None, None, None, None, None, None, None, None, Option(summary.period), None, None, None, None, None, Option(summary.funnel), Option(summary.dspec), None, Option(summary.genieVer)),
                MEEdata(measures), summary.tags);
        }
    }
}