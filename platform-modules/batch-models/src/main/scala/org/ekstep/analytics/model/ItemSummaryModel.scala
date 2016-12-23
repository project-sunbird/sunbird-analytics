package org.ekstep.analytics.model

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.MeasuredEvent
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.PData
import org.ekstep.analytics.framework.Dimensions
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Context
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.AlgoInput
import org.ekstep.analytics.framework.Input
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.ItemKey
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.DtRange
import org.ekstep.analytics.framework.GData
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.RegisteredTag
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.DataFilter
import scala.collection.mutable.ListBuffer
import org.ekstep.analytics.framework.Period
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.DerivedEvent
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.JSONUtils
import scala.collection.immutable.Nil
import org.ekstep.analytics.framework.InCorrectRes

case class InputEventsItemSummary(ik: ItemKey, events: Buffer[ItemUsageMetricsSummary]) extends Input with AlgoInput
case class ItemUsageMetricsSummary(ik: ItemKey, total_ts: Double, total_count: Int, avg_ts: Double, correct_res_count: Int, correct_res: List[String], inc_res_count: Int, m_incorrect_res: List[InCorrectRes], dt_range: DtRange, syncts: Long) extends AlgoOutput;

object ItemSummaryModel extends IBatchModelTemplate[DerivedEvent, InputEventsItemSummary, ItemUsageMetricsSummary, MeasuredEvent] with Serializable {

    val className = "org.ekstep.analytics.model.ItemSummaryModel"
    override def name: String = "ItemSummaryModel"

    private def _computeMetrics(events: Buffer[ItemUsageMetricsSummary], ik: ItemKey): ItemUsageMetricsSummary = {

        val firstEvent = events.sortBy { x => x.dt_range.from }.head;
        val lastEvent = events.sortBy { x => x.dt_range.to }.last;
        val ik = firstEvent.ik;

        val date_range = DtRange(firstEvent.dt_range.from, lastEvent.dt_range.to);
        val total_ts = CommonUtil.roundDouble(events.map { x => x.total_ts }.sum, 2);
        val total_count = events.map { x => x.total_count }.sum
        val avg_ts = CommonUtil.roundDouble(total_ts / total_count, 2)
        val correct_res = events.flatMap { x => x.correct_res }.distinct.toList
        val correct_res_count = events.map { x => x.correct_res_count }.sum
        val inc_res_count = events.map { x => x.inc_res_count }.sum
        val m_incorrect_res = events.flatMap { x => x.m_incorrect_res }.groupBy(x => (x.resp, x.mmc)).map(f => InCorrectRes(f._1._1, f._1._2, f._2.map(y => y.count).sum)).toList;
        ItemUsageMetricsSummary(ik, total_ts, total_count, avg_ts, correct_res_count, correct_res, inc_res_count, m_incorrect_res, date_range, lastEvent.syncts);
    }
    
    private def _getItemUsageSummary(event: DerivedEvent, period: Int, tagId: String, content_id: String, item_id: String): ItemUsageMetricsSummary = {

        val ik = ItemKey(period, tagId, content_id, item_id);
        val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        val total_ts = eksMap.get("timeSpent").get.asInstanceOf[Double];
        val total_count = 1;
        val avg_ts = total_ts;
        val pass = eksMap.get("pass").get.asInstanceOf[String]
        val res = if (eksMap.get("res").isDefined) eksMap.get("res").get.asInstanceOf[List[String]] else List[String]();
        val mmc = if (eksMap.get("mmc").isDefined) eksMap.get("mmc").get.asInstanceOf[List[String]] else List();
        val correct_res = if (StringUtils.equals("Yes", pass) && Nil != res) res.asInstanceOf[List[String]] else List[String]()
        val correct_res_count: Int = if (StringUtils.equals("Yes", pass)) 1 else 0
        val inc_res_count: Int = if (StringUtils.equals("No", pass)) 1 else 0;
        val m_incorrect_res = if (inc_res_count == 1) res.map { x => InCorrectRes(x, mmc, 1) } else List[InCorrectRes]();
        ItemUsageMetricsSummary(ik, total_ts, total_count, avg_ts, correct_res_count, correct_res, inc_res_count, m_incorrect_res, event.context.date_range, event.syncts);
    }

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[InputEventsItemSummary] = {
        val configMapping = sc.broadcast(config);
        val tags = sc.cassandraTable[RegisteredTag](Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS).filter { x => true == x.active }.map { x => x.tag_id }.collect
        val registeredTags = if (tags.nonEmpty) tags; else Array[String]();

        val sessionEvents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_ITEM_SUMMARY")));

        val normalizeEvents = sessionEvents.map { event =>

            var list: ListBuffer[ItemUsageMetricsSummary] = ListBuffer[ItemUsageMetricsSummary]();
            val period = CommonUtil.getPeriod(event.context.date_range.to, Period.DAY);
            val content_id = event.content_id.get
            val item_id = event.edata.eks.asInstanceOf[Map[String, AnyRef]].get("itemId").get.asInstanceOf[String]
            // For all
            list += _getItemUsageSummary(event, period, "all", content_id, item_id);
            val tags = CommonUtil.getValidTags(event, registeredTags);
            for (tag <- tags) {
                list += _getItemUsageSummary(event, period, tag, content_id, item_id);
            }
            list.toArray;
        }.flatMap { x => x.map { x => x } };

        normalizeEvents.map { x => (x.ik, Buffer(x)) }
            .partitionBy(new HashPartitioner(JobContext.parallelization))
            .reduceByKey((a, b) => a ++ b).map { x => InputEventsItemSummary(x._1, x._2) };
    }

    override def algorithm(data: RDD[InputEventsItemSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ItemUsageMetricsSummary] = {

        data.map { x =>
            _computeMetrics(x.events, x.ik);
        }
    }

    override def postProcess(data: RDD[ItemUsageMetricsSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        data.map { itSumm =>
            val mid = CommonUtil.getMessageId("ME_ITEM_USAGE_SUMMARY", itSumm.ik.tag + itSumm.ik.period + itSumm.ik.content_id + itSumm.ik.item_id, "DAY", itSumm.syncts);
            val measures = Map(
                "total_ts" -> itSumm.total_ts,
                "total_count" -> itSumm.total_count,
                "avg_ts" -> itSumm.avg_ts,
                "correct_res_count" -> itSumm.correct_res_count,
                "inc_res_count" -> itSumm.inc_res_count,
                "incorrect_res" -> itSumm.m_incorrect_res,
                "correct_res" -> itSumm.correct_res
                )

            MeasuredEvent("ME_ITEM_USAGE_SUMMARY", System.currentTimeMillis(), itSumm.syncts, "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ItemSummaryModel").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "DAY").asInstanceOf[String], itSumm.dt_range),
                Dimensions(None, None, None, None, None, None, None, None, None, Option(itSumm.ik.tag), Option(itSumm.ik.period), Option(itSumm.ik.content_id), None, Option(itSumm.ik.item_id)),
                MEEdata(measures));
        }
    }

}