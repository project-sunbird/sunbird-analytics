package org.ekstep.analytics.updater

import java.io.FileWriter
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.MEEdata
import org.ekstep.analytics.framework.MeasuredEvent
import scala.collection.immutable.HashMap.HashTrieMap
import org.ekstep.analytics.framework.Event
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.model.SparkGraphSpec

class TestUpdateTemplateSnapshotDB extends SparkGraphSpec(null) {

    override def beforeAll() {
        super.beforeAll();
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE local_creation_metrics_db.template_snapshot_metrics");
        }
    }

    "TemplateSnapshotMetric" should "generate All Template metrics data" in {
        val query2 = "CREATE (temp:domain{IL_FUNC_OBJECT_TYPE:'Content',contentType:'Template',category:['domain'],name:'test-temp',createdBy:'test-author1', IL_UNIQUE_ID:'test-template1'}) RETURN temp"
        GraphQueryDispatcher.dispatch(query2)
        val query1 = "CREATE (as:domain{IL_FUNC_OBJECT_TYPE:'AssessmentItem', IL_UNIQUE_ID:'test-question1',template:'test-template1'}) RETURN as"
        GraphQueryDispatcher.dispatch(query1)
        UpdateTemplateSnapshotDB.execute(sc.makeRDD(List(Empty())), None)
        val data = sc.cassandraTable[TemplateSnapshotMetrics](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.TEMPLATE_SNAPSHOT_METRICS_TABLE).collect
        data.length should be(3)
        data.map { x => x.d_template_id }.foreach { x =>
            x.nonEmpty should be(true)
        }
        val test_IdData = data.filter { x => "test-template1".equals(x.d_template_id) }.last
        test_IdData.d_template_id should be("test-template1")
        test_IdData.template_name should be("test-temp")
        test_IdData.author_id should be("test-author1")
        test_IdData.category should be("domain")
        test_IdData.content_count should be(3)
        test_IdData.question_count should be(1)
    }

}