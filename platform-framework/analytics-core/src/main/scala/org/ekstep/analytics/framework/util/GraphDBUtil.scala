package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.GraphQueryParams._
import org.ekstep.analytics.framework.DataNode
import org.neo4j.driver.v1.Session
import org.neo4j.driver.v1.Record
import scala.collection.JavaConverters._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.JobContext
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.RelationshipDirection
import org.ekstep.analytics.framework.GraphRelation

object GraphDBUtil {

	implicit val className = "org.ekstep.analytics.framework.util.GraphDBUtil"

	def createNode(node: DataNode)(implicit session: Session) {
		val props = removeKeyQuotes(JSONUtils.serialize(Map(UNIQUE_KEY -> node.identifier) ++ node.metadata.getOrElse(Map())));
		val createQuery = StringBuilder.newBuilder;
		createQuery.append(CREATE).append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE)
			.append(node.labels.get.mkString(":")).append(props).append(CLOSE_COMMON_BRACKETS)

		val query = createQuery.toString
		executeQuery(query);
	}

	def createNodes(nodes: RDD[DataNode])(implicit session: Session) {
		val fullQuery = StringBuilder.newBuilder;
		fullQuery.append(CREATE)
		val nodesQuery = nodes.map { x =>
			val nodeQuery = StringBuilder.newBuilder;
			nodeQuery.append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE_WITHOUT_COLON).append(x.identifier).append(COLON)
				.append(x.labels.get.mkString(":"))
			val props = removeKeyQuotes(JSONUtils.serialize(Map(UNIQUE_KEY -> x.identifier) ++ x.metadata.getOrElse(Map())));
			nodeQuery.append(props).append(CLOSE_COMMON_BRACKETS);
			nodeQuery.toString
		}.collect().mkString(",")
		val query = fullQuery.append(nodesQuery).toString
		executeQuery(query)
	}

	def deleteNodes(metadata: Option[Map[String, AnyRef]], labels: Option[List[String]])(implicit session: Session) {
		if (metadata.isEmpty && labels.isEmpty) {
			JobLogger.log("GraphDBUtil.deleteNodes - No metadata or labels to delete nodes.");
		} else {
			val deleteQuery = StringBuilder.newBuilder;
			deleteQuery.append(MATCH).append(getLabelsQuery(labels))

			val props = removeKeyQuotes(JSONUtils.serialize(metadata.getOrElse(Map())));
			deleteQuery.append(props).append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE)
				.append(DETACH_DELETE).append(BLANK_SPACE).append(DEFAULT_CYPHER_NODE_OBJECT);

			val query = deleteQuery.toString;
			executeQuery(query);
		}

	}

	def addRelations(relations: RDD[GraphRelation])(implicit session: Session) {
		val relQueries = relations.map { x => addRelationQuery(x.startNode, x.endNode, x.relation, x.direction, x.metadata) }.collect().toList;
		relQueries.filter { x => StringUtils.isNotBlank(x) }.map { x => executeQuery(x) };
	}
	
	def addRelationQuery(startNode: DataNode, endNode: DataNode, relation: String, direction: String, metadata: Option[Map[String, AnyRef]] = None): String = {
		if (null != startNode && null != endNode && StringUtils.isNotBlank(relation) && StringUtils.isNotBlank(direction)) {
			val fullQuery = StringBuilder.newBuilder;
			fullQuery.append(MATCH).append(getLabelsQuery(startNode.labels));
			fullQuery.append(getPropsQuery(startNode)).append(")").append(COMMA)
			fullQuery.append(getLabelsQuery(endNode.labels, "aa"))
			fullQuery.append(getPropsQuery(endNode)).append(")")
			fullQuery.append(BLANK_SPACE).append(MERGE).append(BLANK_SPACE);
			fullQuery.append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE_WITHOUT_COLON).append(CLOSE_COMMON_BRACKETS)
			fullQuery.append(getRelationQuery(relation, direction)).append(OPEN_COMMON_BRACKETS).append("aa").append(CLOSE_COMMON_BRACKETS);

			fullQuery.toString;
		} else {
			JobLogger.log("GraphDBUtil.addRelation - required parameters missing");
			"";
		}
	}

	def deleteRelation(startNodeId: String, endNodeId: String, relation: String)(implicit session: Session) {

	}

	def deleteAllRelations(startNodeType: String, endNodeType: String, relation: String)(implicit session: Session) {

	}

	def findNodes(metadata: Map[String, AnyRef], labels: Option[List[String]], limit: Option[Int] = None)(implicit sc: SparkContext, session: Session): RDD[DataNode] = {
		val findQuery = StringBuilder.newBuilder;
		findQuery.append(MATCH).append(getLabelsQuery(labels))

		val props = removeKeyQuotes(JSONUtils.serialize(metadata));
		findQuery.append(props).append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE)
			.append(RETURN).append(BLANK_SPACE).append(DEFAULT_CYPHER_NODE_OBJECT);
		if(!limit.isEmpty) findQuery.append(" LIMIT "+limit.get)
		
		val query = findQuery.toString;
		JobLogger.log("Neo4j Query:" + query);
		val result = session.run(query);
		val nodes = if (null != result) {
			result.list().toArray().map(x => x.asInstanceOf[Record])
				.map { x => x.get(DEFAULT_CYPHER_NODE_OBJECT).asNode() }
				.map { x => toDataNode(x) }.toList
		} else {
			List();
		}
		sc.parallelize(nodes, JobContext.parallelization)
	}

	private def toDataNode(node: org.neo4j.driver.v1.types.Node): DataNode = {
		val metadata = getMetadata(node);
		val labels = node.labels().asScala.toList
		val identifier = metadata.getOrElse(UNIQUE_KEY, "").asInstanceOf[String];
		DataNode(identifier, Option(metadata), Option(labels))
	}

	private def getMetadata(node: org.neo4j.driver.v1.types.Node): Map[String, AnyRef] = {
		var metadata = Map[String, AnyRef]();
		for ((k, v) <- node.asMap().asScala) {
			metadata = metadata ++ Map(k -> v.asInstanceOf[AnyRef])
		}
		metadata;
	}

	private def executeQuery(query: String)(implicit session: Session) {
		try {
			JobLogger.log("Neo4j Query:" + query);
			session.run(query)
		} catch {
			case t: Throwable => t.printStackTrace() // TODO: handle error
		}
	}

	private def getRelationQuery(relation: String, direction: String) : String = {
		if (StringUtils.equals(RelationshipDirection.OUTGOING.toString, direction))
			"-[r:" + relation + "]->";
		else if (StringUtils.equals(RelationshipDirection.INCOMING.toString, direction))
			"<-[r:" + relation + "]-";
		else if (StringUtils.equals(RelationshipDirection.BIDIRECTIONAL.toString, direction))
			"-[r:" + relation + "]-";
		else ""
	}

	private def getLabelsQuery(labels: Option[List[String]], key: String = "ee"): String = {
		val query = StringBuilder.newBuilder;
		if (labels.isEmpty)
			query.append("(").append(key)
		else
			query.append("(").append(key).append(":").append(labels.get.mkString(":"))
		query.toString;
	}

	private def getPropsQuery(node: DataNode): String = {
		val nodeProps = Map(UNIQUE_KEY -> node.identifier) ++ node.metadata.getOrElse(Map());
		removeKeyQuotes(JSONUtils.serialize(nodeProps));
	}

	private def removeKeyQuotes(query: String): String = {
		val regex = """"(\w+)":""";
		return query.replaceAll(regex, "$1:")
	}

}