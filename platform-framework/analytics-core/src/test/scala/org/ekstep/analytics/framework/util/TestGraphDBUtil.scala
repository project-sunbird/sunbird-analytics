package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.BaseSpec
import org.ekstep.analytics.framework.DataNode
import org.ekstep.analytics.framework.SparkGraphSpec

class TestGraphDBUtil extends SparkGraphSpec {
  "GraphDBUtil" should "create a node" in {
		val node = DataNode("analytics_test_node", Option(Map("name"-> "Analytics")), Option(List("User")))
		GraphDBUtil.createNode(node);
		// TODO: add assertion.
	}
	
	"GraphDBUtil" should "find a node" in {
		val nodes = GraphDBUtil.findNodes(Map(), Option(List("User")));
		println("Nodes:", nodes.length);
	}
	
	"GraphDBUtil" should "delete a node" in {
		GraphDBUtil.deleteNodes(None, Option(List("User")));
		// TODO: add assertion.
	}
}