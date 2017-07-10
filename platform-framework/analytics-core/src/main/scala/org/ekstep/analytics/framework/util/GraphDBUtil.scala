package org.ekstep.analytics.framework.util

import scala.collection.JavaConverters._

import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.DataNode
import org.ekstep.analytics.framework.GraphQueryParams._
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.Relation
import org.ekstep.analytics.framework.RelationshipDirection
import org.ekstep.analytics.framework.UpdateDataNode
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher

object GraphDBUtil {

	implicit val className = "org.ekstep.analytics.framework.util.GraphDBUtil"

	def executeQuery(query: String)(implicit sc: SparkContext) {
		GraphQueryDispatcher.dispatch(query);
	}
	
	def createNode(node: DataNode)(implicit sc: SparkContext) {
		val props = removeKeyQuotes(JSONUtils.serialize(Map(UNIQUE_KEY -> node.identifier) ++ node.metadata.getOrElse(Map())));
		val createQuery = StringBuilder.newBuilder;
		createQuery.append(CREATE).append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE)
			.append(node.labels.get.mkString(":")).append(props).append(CLOSE_COMMON_BRACKETS)

		val query = createQuery.toString
		GraphQueryDispatcher.dispatch(query);
	}

	def createNodesQuery(nodes: RDD[DataNode]): String = {
		if (!nodes.isEmpty) {
			val fullQuery = StringBuilder.newBuilder;
			fullQuery.append(CREATE)
			val nodesQuery = nodes.map { x =>
				val nodeQuery = StringBuilder.newBuilder;
				val id = CommonUtil.getMessageId(x.identifier, "", System.currentTimeMillis(), None, None)
				nodeQuery.append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE_WITHOUT_COLON).append(id).append(COLON)
					.append(x.labels.get.mkString(":"))
				val props = removeKeyQuotes(JSONUtils.serialize(Map(UNIQUE_KEY -> x.identifier) ++ x.metadata.getOrElse(Map())));
				nodeQuery.append(props).append(CLOSE_COMMON_BRACKETS);
				nodeQuery.toString
			}.collect().mkString(",")
			fullQuery.append(nodesQuery).toString
		} else "";
	}
	
	def createNodes(nodes: RDD[DataNode])(implicit sc: SparkContext) {
		val query = createNodesQuery(nodes);
		if (StringUtils.isNotBlank(query)) 
			GraphQueryDispatcher.dispatch(query);
	}

	def deleteNodes(metadata: Option[Map[String, AnyRef]], labels: Option[List[String]])(implicit sc: SparkContext) {
		if (metadata.isEmpty && labels.isEmpty) {
			JobLogger.log("GraphDBUtil.deleteNodes - No metadata or labels to delete nodes.");
		} else {
			val deleteQuery = StringBuilder.newBuilder;
			deleteQuery.append(MATCH).append(getLabelsQuery(labels))

			val props = removeKeyQuotes(JSONUtils.serialize(metadata.getOrElse(Map())));
			deleteQuery.append(props).append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE)
				.append(DETACH_DELETE).append(BLANK_SPACE).append(DEFAULT_CYPHER_NODE_OBJECT);

			val query = deleteQuery.toString;
			GraphQueryDispatcher.dispatch(query);
		}

	}
	
	def updateNode(metadata: Option[Map[String, AnyRef]], labels: Option[List[String]], propertyName: String, propertyValue: AnyRef)(implicit sc: SparkContext) {
		if (metadata.isEmpty && labels.isEmpty) {
			JobLogger.log("GraphDBUtil.updateNode - No metadata or labels to update nodes.");
		} else {
			val updateQuery = StringBuilder.newBuilder;
			updateQuery.append(MATCH).append(getLabelsQuery(labels))

			val props = removeKeyQuotes(JSONUtils.serialize(metadata.getOrElse(Map())));
			updateQuery.append(props).append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE)
				.append(SET).append(BLANK_SPACE).append(DEFAULT_CYPHER_NODE_OBJECT)
				.append(DOT).append(propertyName).append(BLANK_SPACE).append(EQUALS)
				.append(BLANK_SPACE).append(propertyValue);

			val query = updateQuery.toString;
			println(query)
			GraphQueryDispatcher.dispatch(query);
		}

	}
	
	def updateNodes(nodes: RDD[UpdateDataNode])(implicit sc: SparkContext) {
	  val fullQuery = StringBuilder.newBuilder;
		if (!nodes.isEmpty) {
			fullQuery.append(MATCH)
			val nodesQuery = nodes.map { x =>
				val nodeQuery = StringBuilder.newBuilder;
				nodeQuery.append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE_WITHOUT_COLON).append(x.identifier).append(COLON)
					.append(x.labels.get.mkString(":"))
				val props = removeKeyQuotes(JSONUtils.serialize(x.metadata.getOrElse(Map())));
				nodeQuery.append(props).append(CLOSE_COMMON_BRACKETS);
				nodeQuery.toString
			}.collect().mkString(",")
			val setsQuery = nodes.map { x =>
				val setQuery = StringBuilder.newBuilder;
				setQuery.append(DEFAULT_CYPHER_NODE_OBJECT).append(x.identifier)
					.append(DOT).append(x.propertyName).append(EQUALS).append(x.propertyValue)
				setQuery.toString
			}.collect().mkString(",")
			val query = fullQuery.append(nodesQuery).append(BLANK_SPACE).append(SET).append(BLANK_SPACE).append(setsQuery).toString
			GraphQueryDispatcher.dispatch(query);
		}

	}

	def addRelations(relations: RDD[Relation])(implicit sc: SparkContext) {
		val relQueries = relations.map { x => addRelationQuery(x.startNode, x.endNode, x.relation, x.direction, x.metadata) }.filter { x => StringUtils.isNotBlank(x) };
		GraphQueryDispatcher.dispatch(relQueries)
	}
	
	def addRelationQuery(startNode: DataNode, endNode: DataNode, relation: String, direction: String, metadata: Option[Map[String, AnyRef]] = None): String = {
		if (null != startNode && null != endNode && StringUtils.isNotBlank(relation) && StringUtils.isNotBlank(direction)) {
			val fullQuery = StringBuilder.newBuilder;
			fullQuery.append(MATCH).append(getLabelsQuery(startNode.labels));
			fullQuery.append(getPropsQuery(startNode)).append(")").append(COMMA)
			fullQuery.append(getLabelsQuery(endNode.labels, "aa"))
			fullQuery.append(getPropsQuery(endNode)).append(")")
			fullQuery.append(BLANK_SPACE).append(CREATE).append(BLANK_SPACE);
			fullQuery.append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE_WITHOUT_COLON).append(CLOSE_COMMON_BRACKETS)
			fullQuery.append(getRelationQuery(relation, direction)).append(OPEN_COMMON_BRACKETS).append("aa").append(CLOSE_COMMON_BRACKETS);
			
			fullQuery.toString;
		} else {
			JobLogger.log("GraphDBUtil.addRelation - required parameters missing");
			"";
		}
	}

	// TODO: This API will delete the given relation from the Graph. We have to enhance this API.
	def deleteRelation(relation: String, direction: String, metadata: Option[Map[String, AnyRef]])(implicit sc: SparkContext) {
		if (StringUtils.isNotBlank(relation) && StringUtils.isNotBlank(direction)) {
			val fullQuery = StringBuilder.newBuilder;
			fullQuery.append(MATCH).append("(ee)").append(getRelationQuery(relation, direction))
			fullQuery.append("(aa)")
			fullQuery.append(BLANK_SPACE).append("DELETE").append(BLANK_SPACE).append("r");
			val query = fullQuery.toString;
			GraphQueryDispatcher.dispatch(query);
		} else {
			JobLogger.log("GraphDBUtil.deleteRelation - required parameters missing");
		}
	}

	def findNodes(metadata: Map[String, AnyRef], labels: Option[List[String]], limit: Option[Int] = None, where: Option[String] = None)(implicit sc: SparkContext): RDD[DataNode] = {
		val findQuery = StringBuilder.newBuilder;
		findQuery.append(MATCH).append(getLabelsQuery(labels))

		val props = removeKeyQuotes(JSONUtils.serialize(metadata));
		findQuery.append(props).append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE)
		if (!where.isEmpty) findQuery.append(where.get).append(BLANK_SPACE)
		findQuery.append(RETURN).append(BLANK_SPACE).append(DEFAULT_CYPHER_NODE_OBJECT);
		if(!limit.isEmpty) findQuery.append(" LIMIT "+limit.get)
		
		val query = findQuery.toString;
		val result = GraphQueryDispatcher.dispatch(query);
		val nodes = if (null != result) {
			result.list().toArray().map(x => x.asInstanceOf[org.neo4j.driver.v1.Record])
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

	def getRelationQuery(relation: String, direction: String) : String = {
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