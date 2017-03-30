package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.framework.util.GraphDBUtil
import org.ekstep.analytics.framework.GraphQueryParams._
import org.ekstep.analytics.framework.RelationshipDirection
import org.ekstep.analytics.model.SparkGraphSpec

class TestContentLanguageRelationModel extends SparkGraphSpec(null) {

    it should "create Language nodes and 'expressedIn' relation with contents and pass the test cases" in {

        val graphConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
            "user" -> AppConf.getConfig("neo4j.bolt.user"),
            "password" -> AppConf.getConfig("neo4j.bolt.password"));

        val deleteQuery = "MATCH (l:Language{}) DETACH DELETE l"
        val findContentNodesQuery =  "MATCH (n:domain) where n.IL_FUNC_OBJECT_TYPE = 'Content' AND n.contentType IN ['Story', 'Game', 'Collection', 'Worksheet'] return n.language, n.IL_UNIQUE_ID"
        val findLanguageNodesQuery = "MATCH (l:Language) RETURN l"
        val contentLangRelationsQuery = "MATCH (n: domain) -[r:expressedIn]-> (l: Language) RETURN r"
        
        val contentNodes = GraphQueryDispatcher.dispatch(graphConfig, findContentNodesQuery).list();
        contentNodes.size() should be(3)
        
        GraphQueryDispatcher.dispatch(graphConfig, deleteQuery);
        
        val langNodesBefore = GraphQueryDispatcher.dispatch(graphConfig, findLanguageNodesQuery).list();
        langNodesBefore.size() should be(0)

        val contentLangRelBefore = GraphQueryDispatcher.dispatch(graphConfig, contentLangRelationsQuery).list;
        contentLangRelBefore.size should be(0)

        ContentLanguageRelationModel.main("{}")(Option(sc));

        // After ContentLanguageRelationModel execution check for number of Language nodes created and contentCount field for first node.
        val langNodesAfter = GraphQueryDispatcher.dispatch(graphConfig, findLanguageNodesQuery).list();
        langNodesAfter.size() should be(3)
        langNodesAfter.get(0).get("l").asMap().get("contentCount") should be(1)
        langNodesAfter.get(0).get("l").asMap().get("liveContentCount") should be(1)
        
        langNodesAfter.get(1).get("l").asMap().get("contentCount") should be(1)
        langNodesAfter.get(1).get("l").asMap().get("liveContentCount") should be(0)

        val contentLangRelAfter = GraphQueryDispatcher.dispatch(graphConfig, contentLangRelationsQuery).list;
        contentLangRelAfter.size should be(3)
        
        // check for relation between specific content & language
        val query1 = "MATCH (n: domain{ IL_UNIQUE_ID:'org.ekstep.ra_ms_52d058e969702d5fe1ae0f00' }) -[r]-> (l: Language { IL_UNIQUE_ID:'other' }) RETURN type(r)"
        val rel1 = GraphQueryDispatcher.dispatch(graphConfig, query1).list;
        rel1.size() should be(1)
        rel1.get(0).asMap().get("type(r)") should be("expressedIn")
        
        val query2 = "MATCH (n: domain{ IL_UNIQUE_ID:'page_1_image_0' }) -[r]-> (l: Language { IL_UNIQUE_ID:'marathi' }) RETURN type(r)"
        val rel2 = GraphQueryDispatcher.dispatch(graphConfig, query2).list;
        rel2.size() should be(0)

        val languageContentRelQuery = "MATCH (l: Language) -[r:expressedIn]-> (n: domain) RETURN r"
        val languageContentRels = GraphQueryDispatcher.dispatch(graphConfig, languageContentRelQuery).list;
        languageContentRels.size should be(0)

        val languageLanguageRelQuery = "MATCH (l1: Language) -[r:expressedIn]-> (l2: Language) RETURN r"
        val languageLanguageRels = GraphQueryDispatcher.dispatch(graphConfig, languageLanguageRelQuery).list;
        languageLanguageRels.size should be(0)

        val contentContentRelQuery = "MATCH (n1: domain) -[r:expressedIn]-> (n2: domain) RETURN r"
        val contentContentRels = GraphQueryDispatcher.dispatch(graphConfig, contentContentRelQuery).list;
        contentContentRels.size should be(0)
    }
    
}