package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.DerivedEvent
import org.joda.time.DateTime
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.job.ReplaySupervisor
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.Dispatcher

class TestContentUsageUpdater extends SparkSpec(null) {

    "Content Usage Updater" should "update the content usage updater db and check the updated fields" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.content_usage_summary_fact");
        }
        
        val rdd = loadFile[DerivedEvent]("src/test/resources/content-usage-updater/content_usage_summaries.log");
        val rdd2 = ContentUsageUpdater.execute(rdd, None);
        val updatedSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "do_30079035").where("d_period=?", 0).where("d_tag=?", "all").first
        updatedSumm.m_total_ts should be(143.89)
        updatedSumm.m_avg_interactions_min should be(22.1)
        updatedSumm.m_total_interactions should be(53)
        updatedSumm.m_total_sessions should be(4)
        updatedSumm.m_avg_ts_session should be(35.97)
    }

}