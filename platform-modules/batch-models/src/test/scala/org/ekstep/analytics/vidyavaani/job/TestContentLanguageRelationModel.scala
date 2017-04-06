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

        val findContentNodesQuery =  "MATCH (n:domain) where n.IL_FUNC_OBJECT_TYPE = 'Content' AND n.contentType IN ['Story', 'Game', 'Collection', 'Worksheet'] return n.language, n.IL_UNIQUE_ID"
        val findLanguageNodesQuery = "MATCH (lan:domain{IL_FUNC_OBJECT_TYPE:'Language'}) RETURN lan"
        val contentLangRelationsQuery = "MATCH (n: domain) -[r:expressedIn]-> (l: domain{IL_FUNC_OBJECT_TYPE:'Language'}) RETURN r"
        
        val contentNodes = GraphQueryDispatcher.dispatch(graphConfig, findContentNodesQuery).list();
        contentNodes.size() should be(3)
        
        val langNodesBefore = GraphQueryDispatcher.dispatch(graphConfig, findLanguageNodesQuery).list();
        langNodesBefore.size() should be(3)

        val contentLangRelBefore = GraphQueryDispatcher.dispatch(graphConfig, contentLangRelationsQuery).list;
        contentLangRelBefore.size should be(0)

        ContentLanguageRelationModel.main("{}")(Option(sc));

        // Check for number of Language nodes created.
        val langNodesAfter = GraphQueryDispatcher.dispatch(graphConfig, findLanguageNodesQuery).list();
        langNodesAfter.size() should be(3)

        // Check for contentCount and liveContentCount
        val queryForEng = "MATCH (l: domain{IL_FUNC_OBJECT_TYPE:'Language', IL_UNIQUE_ID:'lang_en' }) RETURN l"
        val eng = GraphQueryDispatcher.dispatch(graphConfig, queryForEng).list();
        eng.get(0).get("l").asMap().get("contentCount") should be(1)
        eng.get(0).get("l").asMap().get("liveContentCount") should be(1)
        
        val queryForOther = "MATCH (l: domain{IL_FUNC_OBJECT_TYPE:'Language', IL_UNIQUE_ID:'lang_oth' }) RETURN l"
        val other = GraphQueryDispatcher.dispatch(graphConfig, queryForOther).list();
        other.get(0).get("l").asMap().get("contentCount") should be(1)
        other.get(0).get("l").asMap().get("liveContentCount") should be(0)

        val contentLangRelAfter = GraphQueryDispatcher.dispatch(graphConfig, contentLangRelationsQuery).list;
        contentLangRelAfter.size should be(3)
        
        // check for relation between specific content & language
        val query1 = "MATCH (n: domain{ IL_UNIQUE_ID:'org.ekstep.ra_ms_52d058e969702d5fe1ae0f00' }) -[r]-> (l: domain{IL_FUNC_OBJECT_TYPE:'Language', IL_UNIQUE_ID:'lang_oth' }) RETURN type(r)"
        val rel1 = GraphQueryDispatcher.dispatch(graphConfig, query1).list;
        rel1.size() should be(1)
        rel1.get(0).asMap().get("type(r)") should be("expressedIn")
        
        val query2 = "MATCH (n: domain{ IL_UNIQUE_ID:'page_1_image_0' }) -[r]-> (l: domain{IL_FUNC_OBJECT_TYPE:'Language', IL_UNIQUE_ID:'lang_mr' }) RETURN type(r)"
        val rel2 = GraphQueryDispatcher.dispatch(graphConfig, query2).list;
        rel2.size() should be(0)

        val languageContentRelQuery = "MATCH (l: domain{IL_FUNC_OBJECT_TYPE:'Language'}) -[r:expressedIn]-> (n: domain) RETURN r"
        val languageContentRels = GraphQueryDispatcher.dispatch(graphConfig, languageContentRelQuery).list;
        languageContentRels.size should be(0)

        val languageLanguageRelQuery = "MATCH (l1: domain{IL_FUNC_OBJECT_TYPE:'Language'}) -[r:expressedIn]-> (l2: domain{IL_FUNC_OBJECT_TYPE:'Language'}) RETURN r"
        val languageLanguageRels = GraphQueryDispatcher.dispatch(graphConfig, languageLanguageRelQuery).list;
        languageLanguageRels.size should be(0)
    }
    
}