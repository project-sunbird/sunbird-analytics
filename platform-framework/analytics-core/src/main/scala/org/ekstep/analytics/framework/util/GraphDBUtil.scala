package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.GraphQueryParams._
import org.ekstep.analytics.framework.DataNode
import org.neo4j.driver.v1.Session
import org.neo4j.driver.v1.Record
import scala.collection.JavaConverters._

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

    def deleteNodes(metadata: Option[Map[String, AnyRef]], labels: Option[List[String]])(implicit session: Session) {
    	if (metadata.isEmpty && labels.isEmpty) {
    		JobLogger.log("GraphDBUtil.deleteNodes - No metadata or labels to delete nodes.");
    	} else {
    		val deleteQuery = StringBuilder.newBuilder;
    		deleteQuery.append(MATCH)
    		if(labels.isEmpty) 
    			deleteQuery.append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE_WITHOUT_COLON) 
    		else 
    			deleteQuery.append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE).append(labels.get.mkString(":"))
    			
    		val props = removeKeyQuotes(JSONUtils.serialize(metadata));
    		deleteQuery.append(props).append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE)
        	.append(DETACH_DELETE).append(BLANK_SPACE).append(DEFAULT_CYPHER_NODE_OBJECT);
    		
    		val query = deleteQuery.toString;
    		executeQuery(query);
    	}
        
        
        
    }

    def addRelation(startNodeId: String, endNodeId: String, relation: String, relationProperties: Option[Map[String, AnyRef]] = None)(implicit session: Session) {

    }

    def deleteRelation(startNodeId: String, endNodeId: String, relation: String)(implicit session: Session) {

    }

    def deleteAllRelations(startNodeType: String, endNodeType: String, relation: String)(implicit session: Session) {

    }
    
    def findNodes(metadata: Map[String, AnyRef], labels: Option[List[String]])(implicit session: Session) : List[DataNode] = {
    	val findQuery = StringBuilder.newBuilder;
		findQuery.append(MATCH)
		if(labels.isEmpty) 
			findQuery.append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE_WITHOUT_COLON) 
		else 
			findQuery.append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE).append(labels.get.mkString(":"))
			
		val props = removeKeyQuotes(JSONUtils.serialize(metadata));
		findQuery.append(props).append(CLOSE_COMMON_BRACKETS).append(BLANK_SPACE)
    	.append(RETURN).append(BLANK_SPACE).append(DEFAULT_CYPHER_NODE_OBJECT);
		val query = findQuery.toString;
		JobLogger.log("Neo4j Query:" + query);
		val result = session.run(query);
		if (null != result) {
			val nodes = result.list().toArray().map(x => x.asInstanceOf[Record]).map { x => x.get(DEFAULT_CYPHER_NODE_OBJECT).asNode() }
			.map { x => 
				val metadata = getMetadata(x);
				val labels = x.labels().asScala.toList
				val identifier = metadata.getOrElse(UNIQUE_KEY, "").asInstanceOf[String];
				DataNode(identifier, Option(metadata), Option(labels))
			}.toList
			nodes;
		} else {
			List();
		}
		
    }
    
    private def getMetadata(node: org.neo4j.driver.v1.types.Node) : Map[String, AnyRef] = {
    	var metadata = Map[String, AnyRef]();
    	for ((k,v) <- node.asMap().asScala) {
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
    
    private def removeKeyQuotes(query: String) : String = {
    	val regex = """"(\w+)":""";
    	return query.replaceAll(regex, "$1:")
    }
 
}