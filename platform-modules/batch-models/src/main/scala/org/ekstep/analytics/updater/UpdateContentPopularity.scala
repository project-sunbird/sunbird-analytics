package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.Filter
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import java.util.UUID
import org.joda.time.DateTime
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.model.ContentSummary
import org.ekstep.analytics.framework.util.RestUtil
import org.ekstep.analytics.framework.Response
import org.ekstep.analytics.framework.ContentId

/**
 * @author Santhosh
 */
object UpdateContentPopularity extends IBatchModel[MeasuredEvent,Any,Any,MEEvent] with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateContentPopularity"
    
    def execute(data: RDD[MeasuredEvent], jobParams: Option[Map[String, AnyRef]])(implicit sc: SparkContext): RDD[String] = {

        JobLogger.debug("Execute method started", className)
        val contents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_CONTENT_SUMMARY"))).map { x => ContentId(x.dimensions.gdata.get.id) }.distinct();
        val summaries = contents.joinWithCassandraTable[ContentSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_CUMULATIVE_SUMMARY_TABLE);
        JobLogger.debug("Execute method ended", className)
        summaries.map { x =>  
            val url = org.ekstep.analytics.framework.util.Constants.getContentUpdateAPIUrl(x._1.content_id);
            val request = Map("request" -> Map("content" -> Map("popularity" -> x._2.total_num_sessions)));
            val r = RestUtil.patch[Response](url, JSONUtils.serialize(request));
            JSONUtils.serialize(Map("contentId" -> x._1.content_id, "popularity" -> x._2.total_num_sessions, "responseCode" -> r.responseCode, "errMsg" -> r.params.errmsg));
        };
    }
}