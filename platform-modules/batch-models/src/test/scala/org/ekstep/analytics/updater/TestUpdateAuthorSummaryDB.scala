package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.DBUtil
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.model.SparkSpec
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.DerivedEvent

class TestUpdateAuthorSummaryDB extends SparkSpec(null) {

    "TestUpdateAuthorSummaryDB" should "update author metrics DB for a sample input data" in {
        DBUtil.truncateTable(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.AUTHOR_USAGE_METRICS_FACT)

        val input = loadFile[DerivedEvent]("src/test/resources/author-usage-updater/test.log");
        UpdateAuthorSummaryDB.execute(input, Option(Map()))

        val data = sc.cassandraTable[AuthorMetricsFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.AUTHOR_USAGE_METRICS_FACT).map { x => x }.collect
        data.length should be(6)
        data.map { x => x.d_period }.contains(20170520) should be (true);
        data.map { x => x.d_period }.contains(2017721) should be (true);
    }

    it should "update author metrics DB with multiple days data for a author" in {
        DBUtil.truncateTable(Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.AUTHOR_USAGE_METRICS_FACT)

        val input = loadFile[DerivedEvent]("src/test/resources/author-usage-updater/test1.log");
        UpdateAuthorSummaryDB.execute(input, Option(Map()))

        val data = sc.cassandraTable[AuthorMetricsFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.AUTHOR_USAGE_METRICS_FACT).map { x => x }.collect
        data.length should be(9)

        val user459 = data.filter { x => StringUtils.equals("459", x.d_author_id) }
        user459.length should be(5)

        val user452 = data.filter { x => StringUtils.equals("452", x.d_author_id) }
        user452.length should be(4)

        // Day
        val user459_25th = user459.filter { x => 20170525 == x.d_period }.last
        println(JSONUtils.serialize(user459_25th))

        user459_25th.d_period should be(20170525)
        user459_25th.d_author_id should be("459")
        //user459_25th.total_session should be()
    }
}