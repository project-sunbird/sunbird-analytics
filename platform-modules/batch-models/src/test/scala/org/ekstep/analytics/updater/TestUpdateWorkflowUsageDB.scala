package org.ekstep.analytics.updater

import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.framework.util.JSONUtils

class TestUpdateWorkflowUsageDB extends SparkSpec(null) {

    override def beforeAll() {
        super.beforeAll()
        val connector = CassandraConnector(sc.getConf);
        val session = connector.openSession();
        session.execute("TRUNCATE local_content_db.content_usage_summary_fact");
    }

    override def afterAll() {
        //        CassandraConnector(sc.getConf).withSessionDo { session =>
        //            session.execute("DELETE FROM local_content_db.registered_tags WHERE tag_id='1375b1d70a66a0f2c22dd1872b98030cb7d9bacb'");
        //        }
        super.afterAll();
    }

    "UpdateWorkflowUsageDB" should "update all usage suammary db and check the updated fields" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/workflow-usage-updater/test-data.log");
        val rdd2 = UpdateWorkFlowUsageDB.execute(rdd, None);
        for(indexes <- rdd2.collect){
            println(JSONUtils.serialize(indexes))
        }
    }
}