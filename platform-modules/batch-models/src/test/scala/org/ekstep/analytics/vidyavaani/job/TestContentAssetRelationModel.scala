package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.model.SparkGraphSpec

class TestContentAssetRelationModel extends SparkGraphSpec(null) {
  
    it should "create uses relation between asset & content and pass the test cases" in {
        
        val graphConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
            "user" -> AppConf.getConfig("neo4j.bolt.user"),
            "password" -> AppConf.getConfig("neo4j.bolt.password"));
        
        val deleteQuery = "match (n1: domain) - [r: uses] -> (n2: domain) where n2.contentType = 'Asset' delete r"
        val contentAssetRelQuery = "match (n1: domain) - [r: uses] -> (n2: domain) where n2.contentType = 'Asset' return r"
        
        GraphQueryDispatcher.dispatch(graphConfig, deleteQuery);
        val contentAssetRelBefore = GraphQueryDispatcher.dispatch(graphConfig, contentAssetRelQuery).list;
        contentAssetRelBefore.size() should be (0)
        
        ContentAssetRelationModel.main("{}")(Option(sc));
        
        val contentAssetRelAfter = GraphQueryDispatcher.dispatch(graphConfig, contentAssetRelQuery).list;
        contentAssetRelAfter.size should be (12)

        // check for relation between specific content & asset
        val query1 = "MATCH (n: domain{ IL_UNIQUE_ID:'org.ekstep.ra_ms_52d058e969702d5fe1ae0f00' }) -[r]-> (a: domain{ IL_UNIQUE_ID:'page_2_image_0' }) RETURN type(r)"
        val rel1 = GraphQueryDispatcher.dispatch(graphConfig, query1).list;
        rel1.size() should be(1)
        rel1.get(0).asMap().get("type(r)") should be("uses")
        
        // check for contentCount in Asset node
        val query2 = "MATCH (a: domain{ IL_UNIQUE_ID:'page_2_image_0' }) RETURN a"
        val rel2 = GraphQueryDispatcher.dispatch(graphConfig, query2).list;
        rel2.size() should be(1)
        rel2.get(0).get("a").asMap().get("contentCount") should be(3)
        rel2.get(0).get("a").asMap().get("liveContentCount") should be(1)
        
        val query3 = "MATCH (a: domain{ IL_UNIQUE_ID:'page_4_image_0' }) RETURN a"
        val rel3 = GraphQueryDispatcher.dispatch(graphConfig, query3).list;
        rel3.size() should be(1)
        rel3.get(0).get("a").asMap().get("contentCount") should be(3)
        rel3.get(0).get("a").asMap().get("liveContentCount") should be(1)

        val assetContentRelQuery = "match (n1: domain) <- [r: uses] - (n2: domain) where n2.contentType = 'Asset' return r"
        val assetContentRels = GraphQueryDispatcher.dispatch(graphConfig, assetContentRelQuery).list;
        assetContentRels.size should be(0)

        val assetAssetRelQuery = "match (n1: domain) - [r: uses] -> (n2: domain) where n2.contentType = 'Asset' AND n1.contentType = 'Asset' return r"
        val assetAssetRels = GraphQueryDispatcher.dispatch(graphConfig, assetAssetRelQuery).list;
        assetAssetRels.size should be(0)
    }
}