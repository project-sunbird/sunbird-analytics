package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.model.SparkGraphSpec
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher

class TestCreationRecommendationModel extends SparkGraphSpec(null) {
  
    it should "create recommendations for authors and populate the same into cassandra" in {

        val graphConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
            "user" -> AppConf.getConfig("neo4j.bolt.user"),
            "password" -> AppConf.getConfig("neo4j.bolt.password"));
        
        // Running all VV jobs
        ContentLanguageRelationModel.main("{}")(Option(sc));
        ConceptLanguageRelationModel.main("{}")(Option(sc));
        ContentAssetRelationModel.main("{}")(Option(sc));
        AuthorRelationsModel.main("{}")(Option(sc));
        
        val query1 = "MATCH (cnt:domain {IL_FUNC_OBJECT_TYPE:'Content'}) where cnt.contentType in ['Game', 'Worksheet', 'Story', 'Collection'] and cnt.status in ['Live', 'Draft'] WITH count(cnt) as ncount MATCH (usr:User{type: 'author'})-[r:uses]-(cnc:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) WHERE cnc.contentCount > 0 WITH r, ncount, toFloat(r.contentCount)/(toFloat(usr.contentCount)) as confidence, (toFloat(r.contentCount)*toFloat(ncount))/(toFloat(usr.contentCount)*toFloat(cnc.contentCount)) as lift SET r.confidence = confidence, r.lift = lift"
        val query2 = "MATCH (cnt:domain {IL_FUNC_OBJECT_TYPE:'Content'}) where cnt.contentType in ['Game', 'Worksheet', 'Story', 'Collection'] and cnt.status in ['Live', 'Draft'] WITH count(cnt) as ncount MATCH (usr:User{type:'author'})-[r:createdIn]->(lan:domain{IL_FUNC_OBJECT_TYPE:'Language'}) WHERE lan.contentCount > 0 WITH r, ncount, toFloat(r.contentCount)/(toFloat(usr.contentCount)) as confidence, (toFloat(r.contentCount)*toFloat(ncount))/(toFloat(usr.contentCount)*toFloat(lan.contentCount)) as lift SET r.confidence = confidence, r.lift = lift"
        val query3 = "MATCH (cnt:domain {IL_FUNC_OBJECT_TYPE:'Content'}) where cnt.contentType in ['Game', 'Worksheet', 'Story', 'Collection'] and cnt.status in ['Live', 'Draft'] WITH count(cnt) as ncount MATCH (usr:User{type:'author'})-[r:uses]->(cntt:ContentType) WHERE cntt.contentCount > 0 WITH r, ncount, toFloat(r.contentCount)/(toFloat(usr.contentCount)) as confidence, (toFloat(r.contentCount)*toFloat(ncount))/(toFloat(usr.contentCount)*toFloat(cntt.contentCount)) as lift SET r.confidence = confidence, r.lift = lift"
        
        GraphQueryDispatcher.dispatch(graphConfig, query1);
        GraphQueryDispatcher.dispatch(graphConfig, query2);
        GraphQueryDispatcher.dispatch(graphConfig, query3);
        
        CreationRecommendationModel.main("{}")(Option(sc));
    }
}