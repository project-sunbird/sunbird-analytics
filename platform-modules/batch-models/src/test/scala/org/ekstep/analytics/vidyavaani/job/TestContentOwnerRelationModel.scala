package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.GraphDBUtil
import org.ekstep.analytics.framework.GraphQueryParams._
import org.ekstep.analytics.framework.RelationshipDirection
import org.ekstep.analytics.model.SparkGraphSpec

class TestContentOwnerRelationModel extends SparkGraphSpec(null) {

    it should "create Owner nodes and 'createdBy' relation with contents and pass test cases" in {

        val graphConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
            "user" -> AppConf.getConfig("neo4j.bolt.user"),
            "password" -> AppConf.getConfig("neo4j.bolt.password"));
     
        val getRelationsQuery = getRelQuery("domain", "Owner")
        
        val contentNodes = GraphDBUtil.findNodes(Map("IL_FUNC_OBJECT_TYPE" -> "Content"), Option(List("domain")));
        contentNodes.count() should be(7)
        
        GraphDBUtil.deleteNodes(None, Option(List(ContentOwnerRelationModel.NODE_NAME)))
        
        val ownerNodesBefore = GraphDBUtil.findNodes(Map(), Option(List(ContentOwnerRelationModel.NODE_NAME)));
        ownerNodesBefore.count() should be(0)

        val contentOwnerRelBefore = GraphQueryDispatcher.dispatch(graphConfig, getRelationsQuery).list;
        contentOwnerRelBefore.size should be(0)

        ContentOwnerRelationModel.main("{}")(Option(sc));

        val ownerNodesAfter = GraphDBUtil.findNodes(Map(), Option(List(ContentOwnerRelationModel.NODE_NAME)));
        ownerNodesAfter.count() should be(1)
      
        val contentOwnerRelAfter = GraphQueryDispatcher.dispatch(graphConfig, getRelationsQuery).list;
        contentOwnerRelAfter.size should be(3)

        // check for relation between specific content & owner
        val query1 = getRelTypeQuery("domain", "org.ekstep.ra_ms_52d058e969702d5fe1ae0f00", "Owner", "290")
        val rel1 = GraphQueryDispatcher.dispatch(graphConfig, query1).list;
        rel1.size() should be(1)
        rel1.get(0).asMap().get("type(r)") should be("createdBy")
        
        val query2 = getRelTypeQuery("domain", "page_1_image_0", "Owner", "290")
        val rel2 = GraphQueryDispatcher.dispatch(graphConfig, query2).list;
        rel2.size() should be(0)
        
        val ownerContentRelQuery = getRelQuery("Owner", "domain")
        val ownerContentRels = GraphQueryDispatcher.dispatch(graphConfig, ownerContentRelQuery).list;
        ownerContentRels.size should be(0)

        val ownerOwnerRelQuery = getRelQuery("Owner", "Owner")
        val ownerOwnerRels = GraphQueryDispatcher.dispatch(graphConfig, ownerOwnerRelQuery).list;
        ownerOwnerRels.size should be(0)

        val contentContentRelQuery = getRelQuery("domain", "domain")
        val contentContentRels = GraphQueryDispatcher.dispatch(graphConfig, contentContentRelQuery).list;
        contentContentRels.size should be(0)
    }
    
    def getRelQuery(startNode: String, endNode: String): String = {
        
        val relationsQuery = StringBuilder.newBuilder;
        relationsQuery.append(MATCH).append(BLANK_SPACE).append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE).append(BLANK_SPACE)
        .append(startNode).append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE)
        .append(GraphDBUtil.getRelationQuery(ContentOwnerRelationModel.RELATION, RelationshipDirection.OUTGOING.toString)).append(BLANK_SPACE)
        .append(OPEN_COMMON_BRACKETS).append(DEFAULT_CYPHER_NODE_OBJECT_II).append(COLON).append(endNode)
        .append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE).append(RETURN).append(BLANK_SPACE).append(DEFAULT_CYPHER_RELATION_OBJECT)
        
        relationsQuery.toString();
    }
    
    def getRelTypeQuery(startNode: String, startNodeUniqueId: String, endNode: String, endNodeUniqueId: String): String = {
        
        val query = StringBuilder.newBuilder;
        query.append(MATCH).append(BLANK_SPACE).append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE).append(BLANK_SPACE)
        .append(startNode).append(OPEN_CURLY_BRACKETS).append(BLANK_SPACE).append("IL_UNIQUE_ID:").append(SINGLE_QUOTE).append(startNodeUniqueId).append(SINGLE_QUOTE)
        .append(BLANK_SPACE).append(CLOSE_CURLY_BRACKETS).append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE)
        .append("-[r]->").append(BLANK_SPACE)
        .append(OPEN_COMMON_BRACKETS).append(DEFAULT_CYPHER_NODE_OBJECT_II).append(COLON).append(endNode).append(BLANK_SPACE)
        .append(OPEN_CURLY_BRACKETS).append(BLANK_SPACE).append("IL_UNIQUE_ID:").append(SINGLE_QUOTE).append(endNodeUniqueId).append(SINGLE_QUOTE)
        .append(BLANK_SPACE).append(CLOSE_CURLY_BRACKETS)
        .append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE).append(RETURN).append(BLANK_SPACE).append("type").append(OPEN_COMMON_BRACKETS).append(DEFAULT_CYPHER_RELATION_OBJECT).append(CLOSE_COMMON_BRACKETS)

        query.toString();
    }
}