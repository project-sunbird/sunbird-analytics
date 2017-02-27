package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher

class TestContentAssetRelationModel extends SparkSpec(null) {
  
    it should "create relation between asset & content and pass the test cases" in {
        
        val graphConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
            "user" -> AppConf.getConfig("neo4j.bolt.user"),
            "password" -> AppConf.getConfig("neo4j.bolt.password"));
        
        val query1 = "match (n1: domain) - [r: uses] -> (n2: domain) where n2.contentType = 'Asset' delete r"
        GraphQueryDispatcher.dispatch(graphConfig, query1);
        
        val query2 = "match (n1: domain) - [r: uses] -> (n2: domain) where n2.contentType = 'Asset' return r"
        val conAssetRel = GraphQueryDispatcher.dispatch(graphConfig, query2).list;
        conAssetRel.size() should be (0)
        
        ContentAssetRelationModel.main("{}")(Option(sc));
        
        val conOwn = GraphQueryDispatcher.dispatch(graphConfig, query2).list;
        conOwn.size should be > (0)

        val query3 = "match (n1: domain) <- [r: uses] - (n2: domain) where n2.contentType = 'Asset' return r"
        val LanguageContentRels = GraphQueryDispatcher.dispatch(graphConfig, query3).list;
        LanguageContentRels.size should be(0)

        val query4 = "match (n1: domain) - [r: uses] -> (n2: domain) where n2.contentType = 'Asset' AND n1.contentType = 'Asset' return r"
        val contentContentRels = GraphQueryDispatcher.dispatch(graphConfig, query4).list;
        contentContentRels.size should be(0)
    }
}