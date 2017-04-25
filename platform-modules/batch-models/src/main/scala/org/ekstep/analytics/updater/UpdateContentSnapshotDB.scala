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

case class ContentSnapshotSummary(d_period: Int, d_author_id: String, d_partner_id: String, total_author_count: Long, total_author_count_start: Long, active_author_count: Long, active_author_count_start: Long, total_content_count: Long, total_content_count_start: Long, live_content_count: Long, live_content_count_start: Long, review_content_count: Long, review_content_count_start: Long) extends AlgoOutput with Output
case class ContentSnapshotIndex(d_period: Int, d_author_id: String, d_partner_id: String)

object UpdateContentSnapshotDB extends IBatchModelTemplate[DerivedEvent, DerivedEvent, ContentSnapshotSummary, ContentSnapshotSummary] with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateContentSnapshotDB"
    override def name: String = "UpdateContentSnapshotDB"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        DataFilter.filter(data, Filter("eid", "EQ", Option("ME_CONTENT_SNAPSHOT_SUMMARY")));
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentSnapshotSummary] = {

        val periodsList = List(DAY, WEEK, MONTH)
        val currentData = data.map { x =>
            for (p <- periodsList) yield {
                val d_period = CommonUtil.getPeriod(x.syncts, p);
                (ContentSnapshotIndex(d_period, x.dimensions.author_id.get, x.dimensions.partner_id.get), x);
            }
        }.flatMap(f => f)
        val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[ContentSnapshotSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SNAPSHOT_SUMMARY).on(SomeColumns("d_period", "d_author_id", "d_partner_id"));
        val joinedData = currentData.leftOuterJoin(prvData)
        joinedData.map { f =>
            val prevSumm = f._2._2.getOrElse(null)
            val eksMap = f._2._1.edata.eks.asInstanceOf[Map[String, AnyRef]]

            val total_author_count = eksMap.get("total_user_count").get.asInstanceOf[Int].toLong
            val active_author_count = eksMap.get("active_user_count").get.asInstanceOf[Int].toLong
            val total_content_count = eksMap.get("total_content_count").get.asInstanceOf[Int].toLong
            val live_content_count = eksMap.get("live_content_count").get.asInstanceOf[Int].toLong
            val review_content_count = eksMap.get("review_content_count").get.asInstanceOf[Int].toLong

            if (null == prevSumm)
                ContentSnapshotSummary(f._1.d_period, f._1.d_author_id, f._1.d_partner_id, total_author_count, total_author_count, active_author_count, active_author_count, total_content_count, total_content_count, live_content_count, live_content_count, review_content_count, review_content_count)
            else
                ContentSnapshotSummary(f._1.d_period, f._1.d_author_id, f._1.d_partner_id, total_author_count, prevSumm.total_author_count_start, active_author_count, prevSumm.active_author_count_start, total_content_count, prevSumm.total_content_count_start, live_content_count, prevSumm.live_content_count_start, review_content_count, prevSumm.review_content_count_start)
        }
    }

    override def postProcess(data: RDD[ContentSnapshotSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentSnapshotSummary] = {
        // Update the database
        data.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SNAPSHOT_SUMMARY)
        data;
    }
}