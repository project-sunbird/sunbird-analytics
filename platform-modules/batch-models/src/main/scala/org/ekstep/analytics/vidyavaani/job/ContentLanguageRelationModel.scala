package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.framework.IJob
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.DataNode
import org.ekstep.analytics.framework.util.GraphDBUtil
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.RelationshipDirection
import org.ekstep.analytics.framework.Relation
import scala.collection.JavaConversions._
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher

object ContentLanguageRelationModel extends optional.Application with IJob {

    val NODE_NAME = "Language";
    val RELATION = "expressedIn";
    implicit val className = "org.ekstep.analytics.vidyavaani.job.ContentLanguageRelationModel"
    val findQuery =  "MATCH (n:domain) where n.IL_FUNC_OBJECT_TYPE = 'Content' AND n.contentType IN ['Story', 'Game', 'Collection', 'Worksheet'] return n.language, n.IL_UNIQUE_ID" 
    val realtionQuery = "MATCH (n:domain{IL_FUNC_OBJECT_TYPE:'Content'}), (l:Language{}) WHERE n.contentType IN ['Story', 'Game', 'Collection', 'Worksheet'] AND l.IL_UNIQUE_ID IN extract(language IN n.language | lower(language)) CREATE (n)-[r:expressedIn]->(l) RETURN r"
    val deleteQuery = "MATCH (l:Language{}) DETACH DELETE l"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {

        JobLogger.init("ContentLanguageRelationModel")
        JobLogger.start("ContentLanguageRelationModel Started executing", Option(Map("config" -> config)))

        val jobConfig = JSONUtils.deserialize[JobConfig](config);

        if (null == sc.getOrElse(null)) {
            JobContext.parallelization = 10;
            implicit val sparkContext = CommonUtil.getSparkContext(JobContext.parallelization, jobConfig.appName.getOrElse("Vidyavaani Graph Model"));
            try {
                execute()
            } catch {
                case t: Throwable => t.printStackTrace()
            } finally {
                CommonUtil.closeSparkContext();
            }
        } else {
            implicit val sparkContext: SparkContext = sc.getOrElse(null);
            execute();
        }
    }

    private def execute()(implicit sc: SparkContext) {

        val time = CommonUtil.time({
            val graphDBConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"), "user" -> AppConf.getConfig("neo4j.bolt.user"), "password" -> AppConf.getConfig("neo4j.bolt.password"));
            cleanUp(graphDBConfig);
            algorithm(graphDBConfig);
        })
        JobLogger.end("ContentLanguageRelationModel Completed", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> 0, "outputEvents" -> 0, "timeTaken" -> time._1)));
    }

    private def algorithm(graphDBConfig: Map[String, String])(implicit sc: SparkContext) = {

        val contentNodes = GraphQueryDispatcher.dispatch(graphDBConfig, findQuery);
        val res = contentNodes.list().map { x => (x.get("n.language").asList(), x.get("n.IL_UNIQUE_ID").asString()) }
          .map(f => for (i <- f._1) yield (i.toString().toLowerCase(), f._2)).flatMap(f => f)
          .filter(f => StringUtils.isNoneBlank(f._1) && StringUtils.isNoneBlank(f._2))
        val contentLanguage = sc.parallelize(res)
        
        val languages = contentLanguage.groupByKey().map(f => (f._1, f._2.size))
            .map { f =>
                DataNode(f._1.toLowerCase(), Option(Map("name" -> f._1, "contentCount" -> f._2.asInstanceOf[AnyRef])), Option(List(NODE_NAME)));
            }
        GraphDBUtil.createNodes(languages);
        GraphQueryDispatcher.dispatch(graphDBConfig, realtionQuery);
    }
    
    private def cleanUp(graphDBConfig: Map[String, String])(implicit sc: SparkContext) {
        GraphQueryDispatcher.dispatch(graphDBConfig, deleteQuery);
    }
}