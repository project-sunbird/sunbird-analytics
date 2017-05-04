package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.model.SparkGraphSpec
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher

class TestConceptLanguageRelationModel extends SparkGraphSpec(null) {
  
    it should "create 'usedIn' relation between concepts and Language and pass the test cases" in {

        val deleteQuery = "MATCH ()-[r:usedIn]->() DELETE r"
        val relQuery = "MATCH (c: domain{IL_FUNC_OBJECT_TYPE:'Concept'})-[r:usedIn]->(l: domain{IL_FUNC_OBJECT_TYPE:'Language'}) RETURN r"
        val findLanguageNodesQuery = "MATCH (l:domain{IL_FUNC_OBJECT_TYPE:'Language'}) RETURN l"
        val findConceptNodesQuery = "MATCH (c: domain{IL_FUNC_OBJECT_TYPE:'Concept'}) RETURN c"
        
        // Running ContentLanguageRelationModel to create language nodes
        ContentLanguageRelationModel.main("{}")(Option(sc));
        
        val languages = GraphQueryDispatcher.dispatch(findLanguageNodesQuery).list;
        languages.size() should be(3)

        GraphQueryDispatcher.dispatch(deleteQuery)
        
        val conceptLangRelBefore = GraphQueryDispatcher.dispatch(relQuery).list;
        conceptLangRelBefore.size should be(0)
        
        ConceptLanguageRelationModel.main("{}")(Option(sc));
        
        // check for contentcount & liveContentCount on Concept Node
        val concepts = GraphQueryDispatcher.dispatch(findConceptNodesQuery).list;
        concepts.size() should be(1)
        concepts.get(0).get("c").asMap().get("contentCount") should be(1)
        concepts.get(0).get("c").asMap().get("liveContentCount") should be(1)
        
        val conceptLangRelAfter = GraphQueryDispatcher.dispatch(relQuery).list;
        conceptLangRelAfter.size should be (languages.size()*concepts.size())
        
        // check for relation and contentCount between specific concept & language
        val query1 = "MATCH (n: domain{ IL_UNIQUE_ID:'Num:C1:SC1' }) -[r:usedIn]-> (l: domain{IL_FUNC_OBJECT_TYPE:'Language', IL_UNIQUE_ID:'lang_oth' }) RETURN r"
        val res1 = GraphQueryDispatcher.dispatch(query1).list();
        res1.size() should be(1)
        res1.get(0).get("r").asMap().get("contentCount") should be(0)
        res1.get(0).get("r").asMap().get("liveContentCount") should be(0)
        
        val query2 = "MATCH (n: domain{ IL_UNIQUE_ID:'Num:C1:SC1' }) -[r:usedIn]-> (l: domain{IL_FUNC_OBJECT_TYPE:'Language', IL_UNIQUE_ID:'lang_en' }) RETURN r"
        val res2 = GraphQueryDispatcher.dispatch(query2).list();
        res2.size() should be(1)
        res2.get(0).get("r").asMap().get("contentCount") should be(1)
        res2.get(0).get("r").asMap().get("liveContentCount") should be(1)
        
    }
}