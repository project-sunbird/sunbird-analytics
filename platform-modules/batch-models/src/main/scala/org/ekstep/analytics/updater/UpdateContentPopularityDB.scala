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
import org.ekstep.analytics.framework.util.RestUtil
import org.ekstep.analytics.framework.Response
import org.ekstep.analytics.framework.ContentId

case class PopularityUpdaterInput(contentId: String, contentSummary: ContentUsageSummaryFact) extends AlgoInput
case class PopularityUpdaterOutut(contentId: String, popularity: Double, reponseCode: String, errorMsg: Option[String]) extends AlgoOutput with Output

/**
 * @author Santhosh
 */
object UpdateContentPopularityDB extends IBatchModelTemplate[DerivedEvent, PopularityUpdaterInput, PopularityUpdaterOutut, PopularityUpdaterOutut] with Serializable {

    val className = "org.ekstep.analytics.updater.UpdateContentPopularityDB"
    override def name: String = "UpdateContentPopularityDB"

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PopularityUpdaterInput] = {
        val contents = DataFilter.filter(data, Filter("eid", "EQ", Option("ME_CONTENT_USAGE_SUMMARY"))).map { x => ContentUsageSummaryIndex(0, x.dimensions.content_id.get, "all") }.distinct();
        val summaries = contents.joinWithCassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).on(SomeColumns("d_period", "d_content_id", "d_tag"));
        summaries.map { x => PopularityUpdaterInput(x._1.d_content_id, x._2) }
    }

    override def algorithm(data: RDD[PopularityUpdaterInput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PopularityUpdaterOutut] = {
        data.map { x =>
            val url = Constants.getContentUpdateAPIUrl(x.contentId);
            val request = Map("request" -> Map("content" -> Map("popularity" -> x.contentSummary.m_total_ts)));
            val r = RestUtil.patch[Response](url, JSONUtils.serialize(request));
            PopularityUpdaterOutut(x.contentId, x.contentSummary.m_total_ts, r.responseCode, r.params.errmsg)
        };
    }

    override def postProcess(data: RDD[PopularityUpdaterOutut], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[PopularityUpdaterOutut] = {
        data
    }
}