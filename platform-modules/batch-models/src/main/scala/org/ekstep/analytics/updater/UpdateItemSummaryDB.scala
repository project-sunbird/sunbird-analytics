package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.CommonUtil
import java.util.Calendar
import java.text.SimpleDateFormat
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Period._
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.util.ItemUsageSummaryFact
import org.ekstep.analytics.util.ItemUsageSummaryIndex
import org.ekstep.analytics.framework.InCorrectRes
import org.ekstep.analytics.framework.conf.AppConf

case class ItemUsageSummaryFact_T(d_period: Int, d_tag: String, d_content_id: String, d_item_id: String, d_app_id: String, d_channel_id: String, m_total_ts: Double, m_total_count: Int, m_correct_res_count: Int, m_inc_res_count: Int, m_correct_res: List[String], m_incorrect_res: List[InCorrectRes], m_avg_ts: Double, m_last_gen_date: Long) extends AlgoOutput

object UpdateItemSummaryDB extends IBatchModelTemplate[DerivedEvent, DerivedEvent, ItemUsageSummaryFact, ItemUsageSummaryIndex] with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateItemSummaryDB"
    override def name: String = "UpdateItemSummaryDB"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        DataFilter.filter(data, Filter("eid", "EQ", Option("ME_ITEM_USAGE_SUMMARY")));
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ItemUsageSummaryFact] = {
        val itemSummary = data.map { x =>

            val period = x.dimensions.period.get;
            val tag = x.dimensions.tag.get;
            val content = x.dimensions.content_id.get;
            val item = x.dimensions.item_id.get;
            val appId = CommonUtil.getAppDetails(x).id
            val channelId = CommonUtil.getChannelId(x)

            val eksMap = x.edata.eks.asInstanceOf[Map[String, AnyRef]]

            val total_ts = eksMap.get("total_ts").get.asInstanceOf[Double]
            val total_count = eksMap.get("total_count").get.asInstanceOf[Int]
            val correct_res_count = eksMap.get("correct_res_count").get.asInstanceOf[Int]
            val inc_res_count = eksMap.get("inc_res_count").get.asInstanceOf[Int]
            val incorrect_res = eksMap.get("incorrect_res").getOrElse(List()).asInstanceOf[List[Map[String, AnyRef]]].filter(p => !p.isEmpty)
            					.map { f => 
            						val resp = f.get("resp") match {
            						  case None => null
            						  case Some(value) => value.asInstanceOf[String]
            						}
            						val mmc = f.get("mmc") match {
            							case None => null
            						  	case Some(value) => value.asInstanceOf[List[String]]
            						}
            						val count = f.get("count") match { 
            							case None => 0
            						  	case Some(value) => value.asInstanceOf[Int]
            						}
            						InCorrectRes(resp, mmc, count);
            					}
            val correct_res = eksMap.get("correct_res").get.asInstanceOf[List[String]]
            val avg_ts = CommonUtil.roundDouble(total_ts / total_count, 2)
            ItemUsageSummaryFact_T(period, tag, content, item, appId, channelId, total_ts, total_count, correct_res_count, inc_res_count, correct_res, incorrect_res, avg_ts, x.context.date_range.to);
        }.cache();
        
        rollup(itemSummary, DAY).union(rollup(itemSummary, WEEK)).union(rollup(itemSummary, MONTH)).union(rollup(itemSummary, CUMULATIVE)).cache();
    }

    override def postProcess(data: RDD[ItemUsageSummaryFact], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ItemUsageSummaryIndex] = {

    	// Update the database
        data.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.ITEM_USAGE_SUMMARY_FACT)
        data.map { x => ItemUsageSummaryIndex(x.d_period, x.d_tag, x.d_content_id, x.d_item_id, x.d_app_id, x.d_channel_id) };
    }

    private def rollup(data: RDD[ItemUsageSummaryFact_T], period: Period): RDD[ItemUsageSummaryFact] = {
        val currentData = data.map { x =>
            val d_period = CommonUtil.getPeriod(x.m_last_gen_date, period);
            (ItemUsageSummaryIndex(d_period, x.d_tag, x.d_content_id, x.d_item_id, x.d_app_id, x.d_channel_id), ItemUsageSummaryFact_T(d_period, x.d_tag, x.d_content_id, x.d_item_id, x.d_app_id, x.d_channel_id, x.m_total_ts, x.m_total_count, x.m_correct_res_count, x.m_inc_res_count, x.m_correct_res, x.m_incorrect_res, x.m_avg_ts, x.m_last_gen_date));
        }.reduceByKey(reduceItemSummary);
        val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[ItemUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.ITEM_USAGE_SUMMARY_FACT).on(SomeColumns("d_period", "d_tag", "d_content_id", "d_item_id", "d_app_id", "d_channel_id"));
        val joinedData = currentData.leftOuterJoin(prvData)
        val rollupSummaries = joinedData.map { x =>
            val index = x._1
            val newSumm = x._2._1
            val prvSumm = x._2._2.getOrElse(ItemUsageSummaryFact(index.d_period, index.d_tag, index.d_content_id, index.d_item_id, index.d_app_id, index.d_channel_id, 0.0, 0, 0, 0, List(), List(), List(), 0.0, List()))
            reduce(prvSumm, newSumm, period);
        }
        rollupSummaries;
    }

    private def reduce(fact1: ItemUsageSummaryFact, fact2: ItemUsageSummaryFact_T, period: Period): ItemUsageSummaryFact = {
        val total_ts = fact2.m_total_ts + fact1.m_total_ts
        val total_count = fact1.m_total_count + fact2.m_total_count
        val correct_res_count = fact1.m_correct_res_count + fact2.m_correct_res_count
        val inc_res_count = fact1.m_inc_res_count + fact2.m_inc_res_count
        val correct_res = (fact1.m_correct_res ++ fact2.m_correct_res).distinct;
        val fact2_inccor_res = fact2.m_incorrect_res.map { x => (x.resp, x.mmc, x.count) }
        val incorrect_res = (fact1.m_incorrect_res ++ fact2_inccor_res ).groupBy(f => (f._1, f._2)).map(f => (f._1._1, f._1._2, f._2.map(y => y._3).sum)).toList;
        val top5_incorrect_res = incorrect_res.sorted(Ordering.by((_: (String, List[String], Int))._3).reverse).take(5);
        val mmc = incorrect_res.map { x => (x._2, x._3) }.filter(p => p._1.length > 0 && p._2 > 0)
        				.map(f => f._1.map { x => (x,f._2) }).flatMap { x => x }
        				.groupBy { f => f._1 }.map { x => (x._1, x._2.map(f => f._2).sum) }.toList;
        val top5_mmc = mmc.sorted(Ordering.by((_: (String, Int))._2).reverse).take(5)
        val avg_ts = CommonUtil.roundDouble((total_ts / total_count), 2)
        ItemUsageSummaryFact(fact1.d_period, fact1.d_tag, fact1.d_content_id, fact1.d_item_id, fact1.d_app_id, fact1.d_channel_id, total_ts, total_count, correct_res_count, inc_res_count, correct_res, incorrect_res, top5_incorrect_res, avg_ts, top5_mmc);
    }

    private def reduceItemSummary(fact1: ItemUsageSummaryFact_T, fact2: ItemUsageSummaryFact_T): ItemUsageSummaryFact_T = {
        val total_ts = fact2.m_total_ts + fact1.m_total_ts
        val total_count = fact1.m_total_count + fact2.m_total_count
        val correct_res_count = fact1.m_correct_res_count + fact2.m_correct_res_count
        val inc_res_count = fact1.m_inc_res_count + fact2.m_inc_res_count
        val correct_res = (fact1.m_correct_res ++ fact2.m_correct_res).distinct
        val incorrect_res = (fact1.m_incorrect_res ++ fact2.m_incorrect_res).groupBy(f => (f.resp, f.mmc)).map(f => InCorrectRes(f._1._1, f._1._2, f._2.map(y => y.count).sum)).toList;
        val avg_ts = CommonUtil.roundDouble((total_ts / total_count), 2)
        ItemUsageSummaryFact_T(fact1.d_period, fact1.d_tag, fact1.d_content_id, fact1.d_item_id, fact1.d_app_id, fact1.d_channel_id, total_ts, total_count, correct_res_count, inc_res_count, correct_res, incorrect_res, avg_ts, fact2.m_last_gen_date);
    }

}