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

        val getNodesQuery = StringBuilder.newBuilder;
        getNodesQuery.append(MATCH).append(BLANK_SPACE).append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE).append(BLANK_SPACE)
        .append(ContentOwnerRelationModel.NODE_NAME).append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE).append(RETURN)
        .append(BLANK_SPACE).append(DEFAULT_CYPHER_NODE_OBJECT)
        
        val getRelationsQuery = getRelQuery("domain", "Owner")
        
        GraphDBUtil.deleteNodes(None, Option(List(ContentOwnerRelationModel.NODE_NAME)))
        
        val ownerNodesBefore = GraphQueryDispatcher.dispatch(graphConfig, getNodesQuery.toString()).list;
        ownerNodesBefore.size() should be(0)

        val contentOwnerRelBefore = GraphQueryDispatcher.dispatch(graphConfig, getRelationsQuery).list;
        contentOwnerRelBefore.size should be(0)

        ContentOwnerRelationModel.main("{}")(Option(sc));

        val ownerNodesAfter = GraphQueryDispatcher.dispatch(graphConfig, getNodesQuery.toString()).list;
        ownerNodesAfter.size() should be > (0)

        val contentOwnerRelAfter = GraphQueryDispatcher.dispatch(graphConfig, getRelationsQuery).list;
        contentOwnerRelAfter.size should be > (0)

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
}