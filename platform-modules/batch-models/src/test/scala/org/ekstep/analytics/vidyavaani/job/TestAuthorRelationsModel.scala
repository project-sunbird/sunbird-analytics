package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.util.GraphDBUtil
import org.ekstep.analytics.framework.GraphQueryParams._
import org.ekstep.analytics.framework.RelationshipDirection
import org.ekstep.analytics.model.SparkGraphSpec
import org.ekstep.analytics.framework.util.JSONUtils

class TestAuthorRelationsModel extends SparkGraphSpec(null) {

    it should "create Author nodes and 'createdBy' relation with contents and pass test cases" in {

        val graphConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
            "user" -> AppConf.getConfig("neo4j.bolt.user"),
            "password" -> AppConf.getConfig("neo4j.bolt.password"));

        val getRelationsQuery = "MATCH (ee: domain {IL_FUNC_OBJECT_TYPE: 'Content'} ) -[r:createdBy]-> (aa: User {type: 'author'}) RETURN r"
        val getConceptAuthorRelQuery = "MATCH (ee: domain {IL_FUNC_OBJECT_TYPE: 'Concept'} ) -[r:usedBy]-> (aa: User {type: 'author'}) RETURN r"

        val contentNodes = GraphDBUtil.findNodes(Map("IL_FUNC_OBJECT_TYPE" -> "Content"), Option(List("domain")));
        contentNodes.count() should be(7)

        GraphDBUtil.deleteNodes(None, Option(List(AuthorRelationsModel.NODE_NAME)))

        val authorNodesBefore = GraphDBUtil.findNodes(Map("type" -> "author"), Option(List(AuthorRelationsModel.NODE_NAME)));
        authorNodesBefore.count() should be(0)

        val contentAuthorRelBefore = GraphQueryDispatcher.dispatch(graphConfig, getRelationsQuery).list;
        contentAuthorRelBefore.size should be(0)

        AuthorRelationsModel.main("{}")(Option(sc));

        val authorNodesAfter = GraphDBUtil.findNodes(Map("type" -> "author"), Option(List(AuthorRelationsModel.NODE_NAME)));
        authorNodesAfter.count() should be(1)

        val contentAuthorRelAfter = GraphQueryDispatcher.dispatch(graphConfig, getRelationsQuery).list;
        contentAuthorRelAfter.size should be(3)

        // check for relation between specific content & owner
        val query1 = getRelTypeQuery("domain", "org.ekstep.ra_ms_52d058e969702d5fe1ae0f00", AuthorRelationsModel.NODE_NAME, "290")
        val rel1 = GraphQueryDispatcher.dispatch(graphConfig, query1).list;
        rel1.size() should be(1)
        rel1.get(0).asMap().get("type(r)") should be("createdBy")

        val query2 = getRelTypeQuery("domain", "page_1_image_0", "User", "290")
        val rel2 = GraphQueryDispatcher.dispatch(graphConfig, query2).list;
        rel2.size() should be(0)

        val authorContentRelQuery = getRelQuery(AuthorRelationsModel.NODE_NAME, "domain")
        val authorContentRels = GraphQueryDispatcher.dispatch(graphConfig, authorContentRelQuery).list;
        authorContentRels.size should be(0)

        val authorOwnerRelQuery = getRelQuery(AuthorRelationsModel.NODE_NAME, AuthorRelationsModel.NODE_NAME)
        val authorOwnerRels = GraphQueryDispatcher.dispatch(graphConfig, authorOwnerRelQuery).list;
        authorOwnerRels.size should be(0)

        val contentContentRelQuery = getRelQuery("domain", "domain")
        val contentContentRels = GraphQueryDispatcher.dispatch(graphConfig, contentContentRelQuery).list;
        contentContentRels.size should be(0)

        val conceptAuthRels = GraphQueryDispatcher.dispatch(graphConfig, getConceptAuthorRelQuery).list;
        conceptAuthRels.size should be(1)

        //val author = GraphQueryDispatcher.dispatch(graphConfig, "MATCH (u:User {IL_UNIQUE_ID:'290'}) RETURN u").next();
        //         println("authors:",author.size());
        //         println("author.get(0):", JSONUtils.serialize(author.get(0)));
        //         println("author.get(0)", author.get(0).get("u"));
        //        author.get(0).get("u").asMap().get("contentCount") should be(3)
        //        author.get(0).get("u").asMap().get("liveContentCount") should be(1)

    }

    def getRelQuery(startNode: String, endNode: String): String = {

        val relationsQuery = StringBuilder.newBuilder;
        relationsQuery.append(MATCH).append(BLANK_SPACE).append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE).append(BLANK_SPACE)
            .append(startNode).append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE)
            .append(GraphDBUtil.getRelationQuery(AuthorRelationsModel.CONTENT_AUTHOR_RELATION, RelationshipDirection.OUTGOING.toString)).append(BLANK_SPACE)
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