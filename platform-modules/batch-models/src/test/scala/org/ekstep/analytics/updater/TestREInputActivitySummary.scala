package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.MeasuredEvent
import com.datastax.spark.connector._

class TestREInputActivitySummary extends SparkSpec(null){
  
    "REInputActivitySummary" should " write activities into learneractivity table to a file" in {
        val rdd = loadFile[MeasuredEvent]("src/test/resources/learner-activity-summary/generic_session_summary_out.log");
        REInputActivitySummary.writeIntoDB(sc, rdd)
        sc.cassandraTable[Activity]("learner_db", "learneractivity").count() should not be (0)
        //OutputDispatcher.dispatch(Dispatcher("file", Map("file" -> "src/test/resources/learner_activity_test_output.log")), rdd2);
    }
}