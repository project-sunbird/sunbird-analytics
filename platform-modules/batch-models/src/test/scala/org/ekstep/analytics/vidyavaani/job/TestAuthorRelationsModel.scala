package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.GraphDBUtil
import org.ekstep.analytics.framework.GraphQueryParams._
import org.ekstep.analytics.framework.RelationshipDirection
import org.ekstep.analytics.model.SparkGraphSpec
import org.ekstep.analytics.framework.util.JSONUtils

class TestAuthorRelationsModel extends SparkGraphSpec(null) {

    it should "create Author nodes and its relation with contents, concepts & languages" in {

        val graphConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
            "user" -> AppConf.getConfig("neo4j.bolt.user"),
            "password" -> AppConf.getConfig("neo4j.bolt.password"));

        val findContentNodesQuery =  "MATCH (n:domain) where n.IL_FUNC_OBJECT_TYPE = 'Content' AND n.contentType IN ['Story', 'Game', 'Collection', 'Worksheet'] return n"
        val deleteAuthorNodesQuery = "MATCH(usr:User{type: 'author'}) DETACH DELETE usr"
        val getAuthorNodesQuery = "MATCH(usr:User{type: 'author'}) RETURN usr"
        val getContentAuthorRelQuery = "MATCH (cnt: domain {IL_FUNC_OBJECT_TYPE: 'Content'} ) -[r:createdBy]-> (usr: User {type: 'author'}) RETURN r"
        val getConceptAuthorRelQuery = "MATCH (cnt: domain {IL_FUNC_OBJECT_TYPE: 'Concept'} ) <-[r:uses]- (usr: User {type: 'author'}) RETURN r"
        val getLanguageAuthorRelQuery = "MATCH (usr:User {type:'author'})-[r:createdIn]->(lan:domain{IL_FUNC_OBJECT_TYPE:'Language'}) RETURN r"
        
        val contentNodes = GraphQueryDispatcher.dispatch(graphConfig, findContentNodesQuery).list;
        contentNodes.size() should be(4)

        GraphQueryDispatcher.dispatch(graphConfig, deleteAuthorNodesQuery)

        // Check for User node and its relations before running AuthorRelationsModel
        val authorNodesBefore = GraphQueryDispatcher.dispatch(graphConfig, getAuthorNodesQuery).list;
        authorNodesBefore.size() should be(0)

        val contentAuthorRelBefore = GraphQueryDispatcher.dispatch(graphConfig, getContentAuthorRelQuery).list;
        contentAuthorRelBefore.size should be(0)
        
        val conceptAuthorRelBefore = GraphQueryDispatcher.dispatch(graphConfig, getConceptAuthorRelQuery).list;
        conceptAuthorRelBefore.size should be(0)
        
        val langAuthorRelBefore = GraphQueryDispatcher.dispatch(graphConfig, getLanguageAuthorRelQuery).list;
        langAuthorRelBefore.size should be(0)

        AuthorRelationsModel.main("{}")(Option(sc));

        // Check for User node and its relations after running AuthorRelationsModel
        val authorNodesAfter = GraphQueryDispatcher.dispatch(graphConfig, getAuthorNodesQuery).list;
        authorNodesAfter.size() should be(2)
        
        // Check for contentCount, liveContentCount & conceptsCount on User node
        val author1 = GraphQueryDispatcher.dispatch(graphConfig, "MATCH (usr:User {type: 'author'}) WHERE usr.IL_UNIQUE_ID = '290' RETURN usr").list;
        author1.size() should be(1)
        author1.get(0).get("usr").asMap().get("contentCount") should be(3)
        author1.get(0).get("usr").asMap().get("liveContentCount") should be(1)
        author1.get(0).get("usr").asMap().get("conceptsCount") should be(1)
        
        val author2 = GraphQueryDispatcher.dispatch(graphConfig, "MATCH (usr:User {type: 'author'}) WHERE usr.IL_UNIQUE_ID = '291' RETURN usr").list;
        author2.size() should be(1)
        author2.get(0).get("usr").asMap().get("contentCount") should be(1)
        author2.get(0).get("usr").asMap().get("liveContentCount") should be(0)
        author2.get(0).get("usr").asMap().get("conceptsCount") should be(0)

        val contentAuthorRelAfter = GraphQueryDispatcher.dispatch(graphConfig, getContentAuthorRelQuery).list;
        contentAuthorRelAfter.size should be(4)
        
        val conceptAuthorRelAfter = GraphQueryDispatcher.dispatch(graphConfig, getConceptAuthorRelQuery).list;
        conceptAuthorRelAfter.size should be(2)

        // Check for contentCount & liveContentCount on User `uses` Concept relationship
        val usesRel1 = GraphQueryDispatcher.dispatch(graphConfig, "MATCH (cnt: domain {IL_FUNC_OBJECT_TYPE: 'Concept'} ) <-[r:uses]- (usr: User {type: 'author'}) WHERE usr.IL_UNIQUE_ID = '290' RETURN r").list;
        usesRel1.size() should be(1)
        usesRel1.get(0).get("r").asMap().get("contentCount") should be(1)
        usesRel1.get(0).get("r").asMap().get("liveContentCount") should be(1)
        
        val usesRel2 = GraphQueryDispatcher.dispatch(graphConfig, "MATCH (cnt: domain {IL_FUNC_OBJECT_TYPE: 'Concept'} ) <-[r:uses]- (usr: User {type: 'author'}) WHERE usr.IL_UNIQUE_ID = '291' RETURN r").list;
        usesRel2.size() should be(1)
        usesRel2.get(0).get("r").asMap().get("contentCount") should be(0)
        usesRel2.get(0).get("r").asMap().get("liveContentCount") should be(0)
        
        val langAuthorRelAfter = GraphQueryDispatcher.dispatch(graphConfig, getLanguageAuthorRelQuery).list;
        langAuthorRelAfter.size should be(6)

        // Check for contentCount & liveContentCount on User `createdIn` Language relationship
        val createdInRel1 = GraphQueryDispatcher.dispatch(graphConfig, "MATCH (usr:User {type:'author'})-[r:createdIn]->(lan:domain{IL_FUNC_OBJECT_TYPE:'Language'}) WHERE usr.IL_UNIQUE_ID = '290' AND lan.IL_UNIQUE_ID = 'lang_en' RETURN r").list;
        createdInRel1.size() should be(1)
        createdInRel1.get(0).get("r").asMap().get("contentCount") should be(1)
        createdInRel1.get(0).get("r").asMap().get("liveContentCount") should be(1)
        
        val createdInRel2 = GraphQueryDispatcher.dispatch(graphConfig, "MATCH (usr:User {type:'author'})-[r:createdIn]->(lan:domain{IL_FUNC_OBJECT_TYPE:'Language'}) WHERE usr.IL_UNIQUE_ID = '291' AND lan.IL_UNIQUE_ID = 'lang_en' RETURN r").list;
        createdInRel2.size() should be(1)
        createdInRel2.get(0).get("r").asMap().get("contentCount") should be(0)
        createdInRel2.get(0).get("r").asMap().get("liveContentCount") should be(0)
        
        // check for relation between specific content & owner
        val query1 = "MATCH (cnt: domain{ IL_UNIQUE_ID:'org.ekstep.ra_ms_52d058e969702d5fe1ae0f00' }) -[r]-> (usr: User { type: 'author', IL_UNIQUE_ID:'290' }) RETURN type(r)"
        val rel1 = GraphQueryDispatcher.dispatch(graphConfig, query1).list;
        rel1.size() should be(1)
        rel1.get(0).asMap().get("type(r)") should be("createdBy")

        val query2 = "MATCH (cnt: domain{ IL_UNIQUE_ID:'org.ekstep.ra_ms_52d058e969702d5fe1ae0f00' }) -[r]-> (usr: User { type: 'author', IL_UNIQUE_ID:'291' }) RETURN type(r)"
        val rel2 = GraphQueryDispatcher.dispatch(graphConfig, query2).list;
        rel2.size() should be(0)

        val authorContentRelQuery = "MATCH (usr: User) -[r:createdBy]-> (cnt: domain) RETURN r"
        val authorContentRels = GraphQueryDispatcher.dispatch(graphConfig, authorContentRelQuery).list;
        authorContentRels.size should be(0)

        val authorAuthorRelQuery = "MATCH (usr1: User) -[r:createdBy]-> (usr2: User) RETURN r"
        val authorOwnerRels = GraphQueryDispatcher.dispatch(graphConfig, authorAuthorRelQuery).list;
        authorOwnerRels.size should be(0)

        val contentContentRelQuery = "MATCH (cnt1: domain) -[r:createdBy]-> (cnt2: domain) RETURN r"
        val contentContentRels = GraphQueryDispatcher.dispatch(graphConfig, contentContentRelQuery).list;
        contentContentRels.size should be(0)
    }
    
    it should "create ContentType nodes and its relation with Authors & Contents" in {

        val graphConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
            "user" -> AppConf.getConfig("neo4j.bolt.user"),
            "password" -> AppConf.getConfig("neo4j.bolt.password"));

        val deleteContentTypeNodes = "MATCH (cntt: ContentType) DETACH DELETE cntt"
        val getContentTypeNodesQuery = "MATCH (cntt: ContentType) RETURN cntt"
        val getContentTypeAuthorRelQuery = "MATCH (usr:User {type:'author'})-[r:uses]->(cntt:ContentType) RETURN r"
        val getContentContentTypeRelQuery = "MATCH (cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'})-[r:isA]->(cntt:ContentType) RETURN r"
        
        GraphQueryDispatcher.dispatch(graphConfig, deleteContentTypeNodes)
        
        // Check for ContentType node and its relations before running AuthorRelationsModel
        val contentTypeNodesBefore = GraphQueryDispatcher.dispatch(graphConfig, getContentTypeNodesQuery).list;
        contentTypeNodesBefore.size() should be(0)

        val contentTypeAuthorRelBefore = GraphQueryDispatcher.dispatch(graphConfig, getContentTypeAuthorRelQuery).list;
        contentTypeAuthorRelBefore.size should be(0)
        
        val contentContentTypeRelBefore = GraphQueryDispatcher.dispatch(graphConfig, getContentContentTypeRelQuery).list;
        contentContentTypeRelBefore.size should be(0)
        
        AuthorRelationsModel.main("{}")(Option(sc));

        // Check for ContentType node and its relations after running AuthorRelationsModel
        val contentTypeNodesAfter = GraphQueryDispatcher.dispatch(graphConfig, getContentTypeNodesQuery).list;
        contentTypeNodesAfter.size() should be(4)
        
        // Check for contentCount & liveContentCount on ContentType node
        val contentType1 = GraphQueryDispatcher.dispatch(graphConfig, "MATCH (cntt: ContentType{ IL_UNIQUE_ID: 'story'}) RETURN cntt").list;
        contentType1.size() should be(1)
        contentType1.get(0).get("cntt").asMap().get("contentCount") should be(3)
        contentType1.get(0).get("cntt").asMap().get("liveContentCount") should be(1)
        
        val contentType2 = GraphQueryDispatcher.dispatch(graphConfig, "MATCH (cntt: ContentType{ IL_UNIQUE_ID: 'worksheet'}) RETURN cntt").list;
        contentType2.size() should be(1)
        contentType2.get(0).get("cntt").asMap().get("contentCount") should be(0)
        contentType2.get(0).get("cntt").asMap().get("liveContentCount") should be(0)
        
        val contentContentTypeRelAfter = GraphQueryDispatcher.dispatch(graphConfig, getContentContentTypeRelQuery).list;
        contentContentTypeRelAfter.size should be(4)
        
        val contentTypeAuthorRelAfter = GraphQueryDispatcher.dispatch(graphConfig, getContentTypeAuthorRelQuery).list;
        contentTypeAuthorRelAfter.size should be(8)

        // Check for contentCount & liveContentCount on User `uses` ContentType relationship
        val usesRel1 = GraphQueryDispatcher.dispatch(graphConfig, "MATCH (usr:User {type:'author'})-[r:uses]->(cntt:ContentType) WHERE usr.IL_UNIQUE_ID = '290' AND cntt.IL_UNIQUE_ID = 'story' RETURN r").list;
        usesRel1.size() should be(1)
        usesRel1.get(0).get("r").asMap().get("contentCount") should be(3)
        usesRel1.get(0).get("r").asMap().get("liveContentCount") should be(1)
        
        val usesRel2 = GraphQueryDispatcher.dispatch(graphConfig, "MATCH (usr:User {type:'author'})-[r:uses]->(cntt:ContentType) WHERE usr.IL_UNIQUE_ID = '291' AND cntt.IL_UNIQUE_ID = 'story' RETURN r").list;
        usesRel2.size() should be(1)
        usesRel2.get(0).get("r").asMap().get("contentCount") should be(0)
        usesRel2.get(0).get("r").asMap().get("liveContentCount") should be(0)
        
    }
}