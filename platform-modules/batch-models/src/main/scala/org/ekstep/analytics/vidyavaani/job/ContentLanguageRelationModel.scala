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

object ContentLanguageRelationModel extends optional.Application with IJob {

    val NODE_NAME = "Language";
    val RELATION = "expressedIn";
    implicit val className = "org.ekstep.analytics.vidyavaani.job.ContentLanguageRelationModel"

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
            GraphDBUtil.deleteNodes(None, Option(List(NODE_NAME)))
            _createLanguageNodeWithRelation();
        })

        JobLogger.end("ContentLanguageRelationModel Completed", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> 0, "outputEvents" -> 0, "timeTaken" -> time._1)));
    }

    private def _createLanguageNodeWithRelation()(implicit sc: SparkContext) = {
        val limit = if (StringUtils.isNotBlank(AppConf.getConfig("graph.content.limit")))
            Option(Integer.parseInt(AppConf.getConfig("graph.content.limit"))) else None

        val contentNodes = GraphDBUtil.findNodes(Map("IL_FUNC_OBJECT_TYPE" -> "Content"), Option(List("domain")), limit);
        val contentLanguage = contentNodes.map { x => x.metadata.getOrElse(Map()) }
            .map(f => (f.getOrElse("language", new java.util.ArrayList()).asInstanceOf[java.util.List[String]], f.getOrElse("IL_UNIQUE_ID", "").asInstanceOf[String]))
            .map(f => for (i <- f._1) yield (i, f._2)).flatMap(f => f)
            .filter(f => StringUtils.isNoneBlank(f._1) && StringUtils.isNoneBlank(f._2))
        
        val languages = contentLanguage.groupByKey().map(f => (f._1, f._2.size))
            .map { f =>
                DataNode(f._1.toLowerCase(), Option(Map("name" -> f._1, "contentCount" -> f._2.asInstanceOf[AnyRef])), Option(List(NODE_NAME)));
            }
        GraphDBUtil.createNodes(languages);
            
        val languageContentRels = contentLanguage.map { f =>
                val startNode = DataNode(f._1.toLowerCase(), None, Option(List(NODE_NAME)));
                val endNode = DataNode(f._2, None, Option(List("domain")));
                Relation(startNode, endNode, RELATION, RelationshipDirection.INCOMING.toString);
            };
        GraphDBUtil.addRelations(languageContentRels);
    }
}