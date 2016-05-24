package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.MeasuredEvent
import org.joda.time.DateTime
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector.cql.CassandraConnector

class TestContentUsageUpdater extends SparkSpec(null) {

    it should "update the content usage updater db and check the updated fields" in {

        val sampleSumm = ContentUsageSummaryFact("org.ekstep.story.hi.vayu", 20167718, false, "Story", "application/vnd.ekstep.ecml-archive", new DateTime(1462675927499L), 19.96d, 2, 9.98d, 7, 21.04d, 0, 0)
        val sampleRDD = sc.parallelize(Array(sampleSumm));
        sampleRDD.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT)

        val rdd = loadFile[MeasuredEvent]("src/test/resources/content_usage_updater/content_usage_updater.log");
        ContentUsageUpdater.execute(rdd, None);

        val updatedSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "org.ekstep.story.hi.vayu").where("d_period=?", 20167718).first
        updatedSumm.m_total_ts should be(39.92)
        updatedSumm.m_avg_interactions_min should be(21.04d)
        updatedSumm.m_total_interactions should be(14)
        updatedSumm.m_total_sessions should be(4)

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("DELETE FROM content_db.contentusagesummary_fact where d_content_id = 'org.ekstep.story.hi.vayu'");
        }

    }
}