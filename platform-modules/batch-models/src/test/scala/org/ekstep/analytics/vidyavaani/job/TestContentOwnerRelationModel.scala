package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.model.SparkSpec
import org.neo4j.spark.Neo4j
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.framework.conf.AppConf

class TestContentOwnerRelationModel extends SparkSpec(null) {

    it should "create Owner nodes and 'createdBy' relation with contents and pass test cases" in {

        val graphConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
            "user" -> AppConf.getConfig("neo4j.bolt.user"),
            "password" -> AppConf.getConfig("neo4j.bolt.password"));

        val query1 = "match (n: domain) - [r: createdBy] - (o: Owner) delete r"
        GraphQueryDispatcher.dispatch(graphConfig, query1);
        val query2 = "match (n: Owner) delete n"
        GraphQueryDispatcher.dispatch(graphConfig, query2);

        val query3 = "match (n: Owner) return n"
        val delOwn = GraphQueryDispatcher.dispatch(graphConfig, query3).list;
        delOwn.size() should be(0)

        val query4 = "match (n: domain) - [r: createdBy] - (o: Owner) return r"
        val delConOwn = GraphQueryDispatcher.dispatch(graphConfig, query4).list;
        delConOwn.size should be(0)

        ContentOwnerRelationModel.main("{}")(Option(sc));

        val query5 = "match (n: Owner) return n"
        val Own = GraphQueryDispatcher.dispatch(graphConfig, query5).list;
        Own.size() should be > (0)

        val conOwn = GraphQueryDispatcher.dispatch(graphConfig, query4).list;
        conOwn.size should be > (0)

        val query6 = "match (o: Owner) - [r: createdBy] -> (n: domain) return r"
        val ownerContentRels = GraphQueryDispatcher.dispatch(graphConfig, query6).list;
        ownerContentRels.size should be(0)

        val query7 = "match (o: Owner) - [r: createdBy] -> (o: Owner) return r"
        val ownerOwnerRels = GraphQueryDispatcher.dispatch(graphConfig, query7).list;
        ownerOwnerRels.size should be(0)

        val query8 = "match (n: domain) - [r: createdBy] -> (n: domain) return r"
        val contentContentRels = GraphQueryDispatcher.dispatch(graphConfig, query8).list;
        contentContentRels.size should be(0)
    }
}