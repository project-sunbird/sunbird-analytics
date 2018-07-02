package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.model.SparkGraphSpec
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.job.consolidated.VidyavaaniModelJobs
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.scalatest.Ignore

@Ignore
class TestCreationRecommendationEnrichmentModel extends SparkGraphSpec(null) {

    it should "set confidence & lift value in the relations between Author -> Concept, Author -> Language & Author -> ContentType" in {

        // Running all VV jobs
        ContentLanguageRelationModel.main("{}")(Option(sc));
        ConceptLanguageRelationModel.main("{}")(Option(sc));
        ContentAssetRelationModel.main("{}")(Option(sc));
        AuthorRelationsModel.main("{}")(Option(sc));
        
        CreationRecommendationEnrichmentModel.main("{}")(Option(sc));
        
        // Check for lift & confidence on User - uses -> Concept relationship
        val usesRels1 = GraphQueryDispatcher.dispatch("MATCH (usr:User{type: 'author'})-[r:uses]-(cnc:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) WHERE cnc.contentCount > 0 AND usr.IL_UNIQUE_ID = '290' RETURN r").list
        usesRels1.size() should be(1)
        usesRels1.get(0).get("r").asMap().get("lift") should be(1.3333333333333333)
        usesRels1.get(0).get("r").asMap().get("confidence") should be(0.3333333333333333)
        
        val usesRels2 = GraphQueryDispatcher.dispatch("MATCH (usr:User{type: 'author'})-[r:uses]-(cnc:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) WHERE cnc.contentCount > 0 AND usr.IL_UNIQUE_ID = '291' RETURN r").list
        usesRels2.size() should be(1)
        usesRels2.get(0).get("r").asMap().get("lift") should be(0.0)
        usesRels2.get(0).get("r").asMap().get("confidence") should be(0.0)
        
        // Check for lift & confidence on User - createdIn -> Language relationship
        val createdInRels1 = GraphQueryDispatcher.dispatch("MATCH (usr:User{type:'author'})-[r:createdIn]->(lan:domain{IL_FUNC_OBJECT_TYPE:'Language'}) WHERE lan.contentCount > 0 AND usr.IL_UNIQUE_ID = '290' AND lan.IL_UNIQUE_ID = 'lang_en' RETURN r").list
        createdInRels1.size() should be(1)
        createdInRels1.get(0).get("r").asMap().get("lift") should be(1.3333333333333333)
        createdInRels1.get(0).get("r").asMap().get("confidence") should be(0.3333333333333333)
        
        val createdInRels2 = GraphQueryDispatcher.dispatch("MATCH (usr:User{type:'author'})-[r:createdIn]->(lan:domain{IL_FUNC_OBJECT_TYPE:'Language'}) WHERE lan.contentCount > 0 AND usr.IL_UNIQUE_ID = '291' AND lan.IL_UNIQUE_ID = 'lang_en' RETURN r").list
        createdInRels2.size() should be(1)
        createdInRels2.get(0).get("r").asMap().get("lift") should be(0.0)
        createdInRels2.get(0).get("r").asMap().get("confidence") should be(0.0)        
        
        // Check for lift & confidence on User - uses -> ContentType relationship
        val usesRels3 = GraphQueryDispatcher.dispatch("MATCH (usr:User{type:'author'})-[r:uses]->(cntt:ContentType{ IL_UNIQUE_ID: 'story'}) WHERE cntt.contentCount > 0 AND usr.IL_UNIQUE_ID = '290' RETURN r").list
        usesRels3.size() should be(1)
        usesRels3.get(0).get("r").asMap().get("lift") should be(1.3333333333333333)
        usesRels3.get(0).get("r").asMap().get("confidence") should be(1.0)
        
        val usesRels4 = GraphQueryDispatcher.dispatch("MATCH (usr:User{type:'author'})-[r:uses]->(cntt:ContentType{ IL_UNIQUE_ID: 'story'}) WHERE cntt.contentCount > 0 AND usr.IL_UNIQUE_ID = '291' RETURN r").list
        usesRels4.size() should be(1)
        usesRels4.get(0).get("r").asMap().get("lift") should be(0.0)
        usesRels4.get(0).get("r").asMap().get("confidence") should be(0.0)
    }
}