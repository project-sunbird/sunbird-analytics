package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.ProfileEvent
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.creation.model.CreationEvent
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.util.JSONUtils
/**
 * @author yuva
 */
class TestUpdateTextbookUsageDB extends SparkSpec(null) {

    override def beforeAll() {
        super.beforeAll()
        val connector = CassandraConnector(sc.getConf);
        val session = connector.openSession();
        session.execute("TRUNCATE " + Constants.PLATFORM_KEY_SPACE_NAME + "." + Constants.TEXTBOOK_SESSION_METRICS_FACT);
    }

    "UpdateTextbookSessionsDB" should "create data in platform db" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/textbook-session-updater/textbook-usage-summary.log");
        UpdateTextbookUsageDB.execute(rdd, None);

        val object1 = sc.cassandraTable[TextbookSessionMetricsFact](Constants.PLATFORM_KEY_SPACE_NAME, Constants.TEXTBOOK_SESSION_METRICS_FACT).first();
        /*object1.time_spent should be(0.0)
        object1.time_diff should be(0.0)
        object1.unit_summary.total_units_added should be(0L)
        object1.unit_summary.total_units_deleted should be(0.0)
        object1.unit_summary.total_units_modified should be(0.0)*/
    }
}