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
import scala.collection.JavaConversions._

object ContentLanguageRelationModel extends optional.Application with IJob {
  
    val NODE_NAME = "Language";
    
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
        _createLanguageNodeWithRelation();
    }
    
    private def _createLanguageNodeWithRelation()(implicit sc: SparkContext, session: Session) = {
        val limit = if(StringUtils.isNotBlank(AppConf.getConfig("graph.content.limit"))) 
        		Option(Integer.parseInt(AppConf.getConfig("graph.content.limit"))) else None 
        
    	val contentNodes = GraphDBUtil.findNodes(Map("IL_FUNC_OBJECT_TYPE" -> "Content"), Option(List("domain")), limit); 
    	
    	val languages = contentNodes.map { x => x.metadata.getOrElse(Map()) }
    	.map(f => (f.getOrElse("language", new java.util.ArrayList()).asInstanceOf[java.util.List[java.lang.String]])).flatMap { x => x }.distinct()
    	.map { f => 
    		DataNode(f.toLowerCase(), Option(Map("name" -> f)), Option(List(NODE_NAME)));
    	}
    	GraphDBUtil.createNodes(languages);
    	
    	val languageContentRels = contentNodes.map { x => x.metadata.getOrElse(Map()) }
    	.map(f => (f.getOrElse("language", new java.util.ArrayList()).asInstanceOf[java.util.List[java.lang.String]], f.getOrElse("IL_UNIQUE_ID", "").asInstanceOf[String]))
    	.map(f => for(i <- f._1) yield (i, f._2)).flatMap(f => f)
    	.filter(f => StringUtils.isNoneBlank(f._1) && StringUtils.isNoneBlank(f._2))
    	.map{ f =>
    		val startNode = DataNode(f._1.toLowerCase(), None, Option(List(NODE_NAME)));
    		val endNode = DataNode(f._2, None, Option(List("domain")));
    		GraphRelation(startNode, endNode, "belongsTo", RelationshipDirection.INCOMING.toString);
    	};
    	GraphDBUtil.addRelations(languageContentRels);
    }
}