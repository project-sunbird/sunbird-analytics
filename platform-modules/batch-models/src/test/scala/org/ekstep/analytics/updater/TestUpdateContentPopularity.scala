package org.ekstep.analytics.updater

import org.scalatest._
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework._
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.CommonUtil
import org.joda.time.DateTime
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector.cql.CassandraConnector

/**
 * @author Santhosh
 */
class TestUpdateContentPopularity extends SparkSpec(null) {

    "UpdateContentPopularity" should "populate total sessions count as popularity on content" in {

        val testContent = ContentUsageSummaryFact(0, "testcontent_1234", "all", DateTime.now, DateTime.now, DateTime.now , 450.0, 4, 112.5, 100, 23.56);
        val testRdd = sc.parallelize(Array(testContent));
        testRdd.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT);
        val rdd = DataFetcher.fetchBatchData[DerivedEvent](Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/content-popularity/test-data.json"))))));
        val rdd2 = UpdateContentPopularityDB.execute(rdd, Option(Map()));
        var out = rdd2.collect();
        out.length should be(1);
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("DELETE FROM content_db.content_usage_summary_fact where d_period = 0 and d_tag = 'all' and d_content_id ='testcontent_1234'");
        }
    }
}