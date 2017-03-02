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

        val getNodesQuery = StringBuilder.newBuilder;
        getNodesQuery.append(MATCH).append(BLANK_SPACE).append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE).append(BLANK_SPACE)
        .append(ContentLanguageRelationModel.NODE_NAME).append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE).append(RETURN)
        .append(BLANK_SPACE).append(DEFAULT_CYPHER_NODE_OBJECT)
        
        val getRelationsQuery = getRelQuery("domain", "Language")
            
        GraphDBUtil.deleteNodes(None, Option(List(ContentLanguageRelationModel.NODE_NAME)))
        
        val langNodesBefore = GraphQueryDispatcher.dispatch(graphConfig, getNodesQuery.toString()).list;
        langNodesBefore.size() should be(0)

        val contentLangRelBefore = GraphQueryDispatcher.dispatch(graphConfig, getRelationsQuery).list;
        contentLangRelBefore.size should be(0)

        ContentLanguageRelationModel.main("{}")(Option(sc));

        val langNodesAfter = GraphQueryDispatcher.dispatch(graphConfig, getNodesQuery.toString()).list;
        langNodesAfter.size() should be > (0)

        val contentLangRelAfter = GraphQueryDispatcher.dispatch(graphConfig, getRelationsQuery).list;
        contentLangRelAfter.size should be > (0)

        val languageContentRelQuery = getRelQuery("Language", "domain")
        val languageContentRels = GraphQueryDispatcher.dispatch(graphConfig, languageContentRelQuery).list;
        languageContentRels.size should be(0)

        val languageLanguageRelQuery = getRelQuery("Language", "Language")
        val languageLanguageRels = GraphQueryDispatcher.dispatch(graphConfig, languageLanguageRelQuery).list;
        languageLanguageRels.size should be(0)

        val contentContentRelQuery = getRelQuery("domain", "domain")
        val contentContentRels = GraphQueryDispatcher.dispatch(graphConfig, contentContentRelQuery).list;
        contentContentRels.size should be(0)
    }
    
    def getRelQuery(startNode: String, endNode: String): String = {
        
        val relationsQuery = StringBuilder.newBuilder;
        relationsQuery.append(MATCH).append(BLANK_SPACE).append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE).append(BLANK_SPACE)
        .append(startNode).append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE)
        .append(GraphDBUtil.getRelationQuery(ContentLanguageRelationModel.RELATION, RelationshipDirection.OUTGOING.toString)).append(BLANK_SPACE)
        .append(OPEN_COMMON_BRACKETS).append(DEFAULT_CYPHER_NODE_OBJECT_II).append(COLON).append(endNode)
        .append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE).append(RETURN).append(BLANK_SPACE).append(DEFAULT_CYPHER_RELATION_OBJECT)
        
        relationsQuery.toString();
    }
}