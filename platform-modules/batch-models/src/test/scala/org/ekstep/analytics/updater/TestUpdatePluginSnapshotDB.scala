package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.updater.PluginSnapshotMetrics
import org.ekstep.analytics.updater.UpdatePluginSnapshotDB
import org.ekstep.analytics.util.Constants

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

class TestUpdatePluginSnapshotDB extends SparkGraphSpec(null) {
    
	override def beforeAll() {
        super.beforeAll();
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE creation_metrics_db.plugin_snapshot_metrics");
        }
    }
    "PluginSnapshotMetric" should "generate All plugin Snapshot matrix data" in {
        val query = "CREATE (plu: domain{code:'test-plugin-1',description:'Write a short description of your lesson',language:['English'],mimeType:'application/vnd.ekstep.plugin-archive',idealScreenSize:'normal',createdOn:'2017-02-02T14:33:20.537+0000',gradeLevel:['Grade 1'],lastUpdatedOn:'2017-06-05T12:08:41.529+0000',SYS_INTERNAL_LAST_UPDATED_ON:'2017-06-12T18:33:41.225+0000',contentType:'Plugin',owner:'Manoj Londhe',lastUpdatedBy:'177',audience:['Learner'],os:['All'],visibility:'Default',IL_SYS_NODE_TYPE:'DATA_NODE',consumerId:'62e15662-bb09-439f-86e2-d65bd84f3c23',portalOwner:'177',mediaType:'content',osId:'org.ekstep.quiz.app',ageGroup:['5-6'],versionKey:'1497292421225',idealScreenDensity:'hdpi',createdBy:'177',compatibilityLevel:1,domain:['literacy'],IL_FUNC_OBJECT_TYPE:'Content',name:'Untitled lesson',IL_UNIQUE_ID:'test-plugin-1',status:'Draft'}) <-[r:uses]- (cnt: domain{IL_FUNC_OBJECT_TYPE:'Content', IL_UNIQUE_ID:'test_content-1', contentType:'story'})"
        GraphQueryDispatcher.dispatch(query)
        UpdatePluginSnapshotDB.execute(sc.makeRDD(List(Empty())), None)
        val data = sc.cassandraTable[PluginSnapshotMetrics](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.PLUGIN_SNAPSHOT_METRICS_TABLE).collect
        data.length should be(3)
        data.map { x => x.d_plugin_id }.foreach { x =>
            x.nonEmpty should be(true)
        }
        val test_IdData = data.last
        test_IdData.d_plugin_id should be("test-plugin-1")
        test_IdData.plugin_name should be("Untitled lesson")
        test_IdData.category should be("")
        test_IdData.author should be("177")
        test_IdData.content_count should be(1)

    }

}