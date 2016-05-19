package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.SessionBatchModel
import org.ekstep.analytics.framework.util.CommonUtil
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.util.JSONUtils

case class GenieSummary(timeSpent: Double, time_stamp: Long, content: Buffer[String], contentCount: Int)

object GenieUsageSummary extends SessionBatchModel[Event] with Serializable {

    def execute(data: RDD[Event], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        val genieLaunchSessions = getGenieLaunchSessions(data);
        //val genieSessions = getGenieSessions(data);
        val summary = genieLaunchSessions.mapValues { x =>
            //            val geStart = x.filter { x => "GE_GENIE_START".equals(x.eid) }(0)
            //            val geEnd = x.filter { x => "GE_GENIE_END".equals(x.eid) }(0)

            val geStart = x(0)
            val geEnd = x.last

            val timeSpent = CommonUtil.getTimeDiff(geStart, geEnd)
            val time_stamp = CommonUtil.getTimestamp(geEnd.ts)
            val content = x.filter { x => "GE_LAUNCH_GAME".equals(x.eid) }.map { x => x.gdata.id }.distinct
            val contentCount = content.size
            GenieSummary(timeSpent.getOrElse(0d), time_stamp, content, contentCount);
        }
        summary.map { x => JSONUtils.serialize(x) };
    }
}