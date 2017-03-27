package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.framework.IJob
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import scala.collection.JavaConversions._

object ConceptLanguageRelationModel extends optional.Application with IJob {
  
    val RELATION = "usedIn";
    implicit val className = "org.ekstep.analytics.vidyavaani.job.ConceptLanguageRelationModel"
    val relationQuery = "MATCH (l:Language), (c:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) OPTIONAL MATCH p=(l)<-[ln:expressedIn]-(n:domain{IL_FUNC_OBJECT_TYPE:'Content'})-[nc:associatedTo]->(c) WHERE lower(n.contentType) IN ['story', 'game', 'collection', 'worksheet'] WITH c, l, CASE WHEN p is null THEN 0 ELSE COUNT(p) END AS cc MERGE (c)-[r:usedIn{contentCount: cc}]->(l) RETURN r"
    val deleteRelQuery = "MATCH ()-[r:usedIn]->() DELETE r"
    
    def main(config: String)(implicit sc: Option[SparkContext] = None) {

        JobLogger.init("ConceptLanguageRelationModel")
        JobLogger.start("ConceptLanguageRelationModel Started executing", Option(Map("config" -> config)))

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
            cleanUp(graphDBConfig)
            algorithm(graphDBConfig)
        })
        JobLogger.end("ConceptLanguageRelationModel Completed", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> 0, "outputEvents" -> 0, "timeTaken" -> time._1)));
    }
    
    private def algorithm(graphDBConfig: Map[String, String])(implicit sc: SparkContext) = {
        GraphQueryDispatcher.dispatch(graphDBConfig, relationQuery);
    }
    
    private def cleanUp(graphDBConfig: Map[String, String])(implicit sc: SparkContext) {
        GraphQueryDispatcher.dispatch(graphDBConfig, deleteRelQuery);
    }
}