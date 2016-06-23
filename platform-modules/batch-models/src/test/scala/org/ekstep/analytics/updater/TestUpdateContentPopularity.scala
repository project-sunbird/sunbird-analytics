package org.ekstep.analytics.updater

import org.scalatest._
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework._
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.model.ContentSummary
import org.joda.time.DateTime
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector.cql.CassandraConnector

/**
 * @author Santhosh
 */
class TestUpdateContentPopularity extends SparkSpec(null) {

    "UpdateContentPopularity" should "populate total sessions count as popularity on content" in {

        val testContent = ContentSummary("org.ekstep.story.hi.nature",DateTime.now,5,0d,0d,100,0d,0d,0d,"Story","application/vnd.ekstep.ecml-archive")
        val testRdd = sc.parallelize(Array(testContent));
        testRdd.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_CUMULATIVE_SUMMARY_TABLE)
        val rdd = DataFetcher.fetchBatchData[DerivedEvent](Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/content-popularity/test-data.json"))))));
        val rdd2 = UpdateContentPopularity.execute(rdd, Option(Map()));
        var out = rdd2.collect();
        out.length should be(1);
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("DELETE FROM content_db.content_cumulative_summary where content_id ='org.ekstep.story.hi.nature'");
        }
    }
}