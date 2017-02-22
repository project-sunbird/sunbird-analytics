package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.framework.IJob
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.neo4j.spark._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.DataNode
import org.ekstep.analytics.framework.util.GraphDBUtil
import org.neo4j.driver.v1.Session
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.RelationshipDirection
import org.ekstep.analytics.framework.GraphRelation


object ContentOwnerRelationModel extends optional.Application with IJob {
	
	val NODE_NAME = "Owner";

    def main(config: String)(implicit sc: Option[SparkContext] = None) {

        val jobConfig = JSONUtils.deserialize[JobConfig](config);

        if (null == sc.getOrElse(null)) {
            JobContext.parallelization = 10;
            implicit val sparkContext = CommonUtil.getSparkContext(JobContext.parallelization, jobConfig.appName.getOrElse("Vidyavaani Neo4j Model"));
            implicit val session: Session = CommonUtil.getGraphDbSession();
            try {
                execute()
            } catch {
                case t: Throwable => t.printStackTrace()
            } finally {
            	CommonUtil.closeGraphDbSession();
                CommonUtil.closeSparkContext();
            }
        } else {
            implicit val sparkContext: SparkContext = sc.getOrElse(null);
            implicit val session: Session = CommonUtil.getGraphDbSession();
            execute();
            CommonUtil.closeGraphDbSession();
        }
    }

    private def execute()(implicit sc: SparkContext, session: Session) {

        GraphDBUtil.deleteNodes(None, Option(List(NODE_NAME)))
        _createOwnerNodeWithRelation();
    }

    private def _createOwnerNodeWithRelation()(implicit sc: SparkContext, session: Session) = {
        val limit = if(StringUtils.isNotBlank(AppConf.getConfig("graph.content.limit"))) 
        		Option(Integer.parseInt(AppConf.getConfig("graph.content.limit"))) else None 
        
    	val contentNodes = GraphDBUtil.findNodes(Map("IL_FUNC_OBJECT_TYPE" -> "Content"), Option(List("domain")), limit); 

    	val owners = contentNodes.map { x => x.metadata.getOrElse(Map()) }
    	.map(f => (f.getOrElse("portalOwner", "").asInstanceOf[String], f.getOrElse("owner", "").asInstanceOf[String]))
    	.groupBy(f => f._1).filter(p => !StringUtils.isBlank(p._1))
    	.map { f => 
    		val identifier = f._1;
    		val namesList = f._2.filter(p => !StringUtils.isBlank(p._2));
    		val name = if(namesList.isEmpty) identifier else namesList.last._2;
    		DataNode(identifier, Option(Map("name" -> name)), Option(List(NODE_NAME)));
    	}

    	GraphDBUtil.createNodes(owners);
    	
    	val ownerContentRels = contentNodes.map { x => x.metadata.getOrElse(Map()) }
    	.map(f => (f.getOrElse("portalOwner", "").asInstanceOf[String], f.getOrElse("IL_UNIQUE_ID", "").asInstanceOf[String]))
    	.filter(f => StringUtils.isNoneBlank(f._1) && StringUtils.isNoneBlank(f._2))
    	.map{ f =>
    		val startNode = DataNode(f._1, None, Option(List("Owner")));
    		val endNode = DataNode(f._2, None, Option(List("domain")));
    		GraphRelation(startNode, endNode, "createdBy", RelationshipDirection.OUTGOING.toString);
    	};
    	GraphDBUtil.addRelations(ownerContentRels);
    	
    }

    private def _createRelation()(implicit session: Session) = {
        val script = """MATCH (a:domain), (b:Owner) WHERE a.IL_FUNC_OBJECT_TYPE = 'Content' AND EXISTS(a.portalOwner) AND a.portalOwner = b.IL_UNIQUE_ID CREATE (a)-[r:createdBy]->(b)"""
        val startNode = DataNode("org.ekstep.takeoff", None, Option(List("domain")));
        val endNode = DataNode("EkStep", None, Option(List("Owner")));
//        GraphDBUtil.addRelation(startNode, endNode, "createdBy", RelationshipDirection.OUTGOING.toString, None);
//        GraphDBUtil.addRelation(startNode, endNode, relation, relationProperties)
    }
}