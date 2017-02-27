package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.model.SparkSpec
import org.neo4j.spark.Neo4j
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher

class TestContentLanguageRelationModel extends SparkSpec(null) {

    it should "create Language nodes and 'belongsTo' relation with contents and pass the test cases" in {

        val graphConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
            "user" -> AppConf.getConfig("neo4j.bolt.user"),
            "password" -> AppConf.getConfig("neo4j.bolt.password"));

        val query1 = "match (n: domain) - [r: expressedIn] - (l: Language) delete r"
        GraphQueryDispatcher.dispatch(graphConfig, query1);
        val query2 = "match (n: Language) delete n"
        GraphQueryDispatcher.dispatch(graphConfig, query2);

        val query3 = "match (n: Language) return n"
        val delOwn = GraphQueryDispatcher.dispatch(graphConfig, query3).list;
        delOwn.size() should be(0)

        val query4 = "match (n: domain) - [r: expressedIn] - (l: Language) return r"
        val delConOwn = GraphQueryDispatcher.dispatch(graphConfig, query4).list;
        delConOwn.size should be(0)

        ContentLanguageRelationModel.main("{}")(Option(sc));

        val query5 = "match (n: Language) return n"
        val Own = GraphQueryDispatcher.dispatch(graphConfig, query5).list;
        Own.size() should be > (0)

        val conOwn = GraphQueryDispatcher.dispatch(graphConfig, query4).list;
        conOwn.size should be > (0)

        val query6 = "match (l: Language) - [r: expressedIn] -> (n: domain) return r"
        val LanguageContentRels = GraphQueryDispatcher.dispatch(graphConfig, query6).list;
        LanguageContentRels.size should be(0)

        val query7 = "match (l: Language) - [r: expressedIn] -> (l: Language) return r"
        val LanguageLanguageRels = GraphQueryDispatcher.dispatch(graphConfig, query7).list;
        LanguageLanguageRels.size should be(0)

        val query8 = "match (n: domain) - [r: expressedIn] -> (n: domain) return r"
        val contentContentRels = GraphQueryDispatcher.dispatch(graphConfig, query8).list;
        contentContentRels.size should be(0)
    }
}