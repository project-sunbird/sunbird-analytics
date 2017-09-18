package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.DerivedEvent
import org.joda.time.DateTime
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector.cql.CassandraConnector

class TestUpdateGenieUsageDB extends SparkSpec(null) {

    "UpdateGenieUsageDB" should "update the genie usage db and check the fields" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE local_content_db.genie_launch_summary_fact");
        }
        
        val rdd = loadFile[DerivedEvent]("src/test/resources/genie-usage-updater/gus_1.log");
        val rdd2 = UpdateGenieUsageDB.execute(rdd, None);

        val cummAllGenieSumm = sc.cassandraTable[GenieUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.GENIE_LAUNCH_SUMMARY_FACT).where("d_period=?", 0).where("d_tag=?", "all").first
        cummAllGenieSumm.m_total_sessions should be(1160L)
        cummAllGenieSumm.m_total_ts should be(848039.74)
        cummAllGenieSumm.m_avg_ts_session should be(731.07)
        cummAllGenieSumm.m_total_devices should be(58)
        cummAllGenieSumm.m_avg_sess_device should be(20)

    }

}