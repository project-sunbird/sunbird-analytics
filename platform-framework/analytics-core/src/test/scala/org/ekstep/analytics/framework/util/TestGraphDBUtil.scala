package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.BaseSpec
import org.ekstep.analytics.framework.DataNode
import org.ekstep.analytics.framework.SparkSpec
import org.ekstep.analytics.framework.Relation
import org.ekstep.analytics.framework.RelationshipDirection
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.GraphQueryParams._
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.SparkGraphSpec

class TestGraphDBUtil extends SparkGraphSpec {

  "GraphDBUtil" should "Create single User node" in {
    GraphDBUtil.deleteNodes(None, Option(List("User")));
    val node = DataNode("analytics_test_node", Option(Map("name" -> "Analytics")), Option(List("User")))
    GraphDBUtil.createNode(node);
    val nodes = GraphDBUtil.findNodes(Map(), Option(List("User")));
    nodes.count() should be(1)
  }

  "GraphDBUtil" should "create multiple User nodes" in {
    GraphDBUtil.deleteNodes(None, Option(List("User")));
    val node1 = DataNode("analytics_test_node_1", Option(Map("name" -> "Analytics_1")), Option(List("User")))
    val node2 = DataNode("analytics_test_node_2", Option(Map("name" -> "Analytics_2")), Option(List("User")))
    val node3 = DataNode("analytics_test_node_3", Option(Map("name" -> "Analytics_3")), Option(List("User")))
    val input = sc.parallelize(List(node1, node2, node3))
    GraphDBUtil.createNodes(input)
    val nodes = GraphDBUtil.findNodes(Map(), Option(List("User")));
    nodes.count() should be(3)
    // check find nodes with limit
    val nodes2 = GraphDBUtil.findNodes(Map(), Option(List("User")), Option(2));
    nodes2.count() should be(2)
  }

  it should "delete nodes" in {
    GraphDBUtil.deleteNodes(None, Option(List("User")));
    val node = DataNode("analytics_test_node", Option(Map("name" -> "Analytics")), Option(List("User")))
    GraphDBUtil.createNode(node);
    val nodesBefore = GraphDBUtil.findNodes(Map(), Option(List("User")));
    nodesBefore.count() should be(1)
    GraphDBUtil.deleteNodes(None, Option(List("User")));
    val nodesAfter = GraphDBUtil.findNodes(Map(), Option(List("User")));
    nodesAfter.count() should be(0)
    // check for empty labels and metadata
    GraphDBUtil.deleteNodes(None, None);
  }

  it should "Add isMemberOf relation between user and team" in {
    GraphDBUtil.deleteNodes(None, Option(List("User")));
    GraphDBUtil.deleteNodes(None, Option(List("Team")));
    val node1 = DataNode("analytics", Option(Map("name" -> "Analytics")), Option(List("Team")))
    val node2 = DataNode("user_1", Option(Map("name" -> "User_1")), Option(List("User")))
    val input = sc.parallelize(List(node1, node2))
    GraphDBUtil.createNodes(input)
    val rel = sc.parallelize(List(Relation(node1, node2, "isMemberOf", RelationshipDirection.INCOMING.toString)))
    GraphDBUtil.addRelations(rel)

    val relationsQuery = StringBuilder.newBuilder;
    relationsQuery.append(MATCH).append(BLANK_SPACE).append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE).append(BLANK_SPACE)
      .append("Team").append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE)
      .append(GraphDBUtil.getRelationQuery("isMemberOf", RelationshipDirection.INCOMING.toString)).append(BLANK_SPACE)
      .append(OPEN_COMMON_BRACKETS).append(DEFAULT_CYPHER_NODE_OBJECT_II).append(COLON).append("User")
      .append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE).append(RETURN).append(BLANK_SPACE).append(DEFAULT_CYPHER_RELATION_OBJECT)
    val rels = GraphQueryDispatcher.dispatch(relationsQuery.toString()).list;
    rels.size() should be(1)
    GraphDBUtil.deleteNodes(None, Option(List("User")));
    GraphDBUtil.deleteNodes(None, Option(List("Team")));
  }

  it should "check addRelationQuery method for returning blank output " in {
    val query = GraphDBUtil.addRelationQuery(null, null, "", "")
    StringUtils.isBlank(query) should be(true)
  }

  it should "check getRelationQuery method" in {
    val incomingRel = GraphDBUtil.getRelationQuery("isMemberOf", RelationshipDirection.INCOMING.toString)
    StringUtils.contains(incomingRel, "<") should be(true)
    val outgoingRel = GraphDBUtil.getRelationQuery("isMemberOf", RelationshipDirection.OUTGOING.toString)
    StringUtils.contains(outgoingRel, ">") should be(true)
    val biDirectionalRel = GraphDBUtil.getRelationQuery("isMemberOf", RelationshipDirection.BIDIRECTIONAL.toString)
    StringUtils.contains(biDirectionalRel, ">") && StringUtils.contains(biDirectionalRel, "<") should be(false)
    val Emptyrel = GraphDBUtil.getRelationQuery("isMemberOf", "")
    StringUtils.isBlank(Emptyrel) should be(true)
  }
}