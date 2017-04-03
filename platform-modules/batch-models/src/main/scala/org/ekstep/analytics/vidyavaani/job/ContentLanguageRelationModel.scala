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
import org.ekstep.analytics.job.IGraphExecutionModel
import org.apache.spark.rdd.RDD
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.Job_Config
import org.ekstep.analytics.util.Constants

object ContentLanguageRelationModel extends IGraphExecutionModel with Serializable {

    override def name(): String = "ContentLanguageRelationModel";
    override implicit val className = "org.ekstep.analytics.vidyavaani.job.ContentLanguageRelationModel";

    val NODE_NAME = "Language";
    val RELATION = "expressedIn";
    var algorithmQueries: List[String] = List();

    val findQuery = "MATCH (cnt:domain{IL_FUNC_OBJECT_TYPE: 'Content'}) WHERE lower(cnt.contentType)IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Draft', 'Review', 'Live'] RETURN cnt.language"

    override def preProcess(input: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {
        val job_config = sc.cassandraTable[Job_Config](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_CONFIG).where("category='vv' AND config_key=?", "content-lang-rel").first
        val cleanupQueries = job_config.config_value.get("cleanupQueries").get
        algorithmQueries = job_config.config_value.get("algorithmQueries").get
        sc.parallelize(cleanupQueries, JobContext.parallelization);
    }

    override def algorithm(ppQueries: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {

        val contentNodes = GraphQueryDispatcher.dispatch(graphDBConfig, findQuery);
        val res = contentNodes.list().map { x => (x.get("cnt.language", new java.util.ArrayList()).asInstanceOf[java.util.List[String]]) }
            .flatMap(f => f).filter(f => StringUtils.isNoneBlank(f))
        val contentLanguage = sc.parallelize(res).map { x => x.toLowerCase() }.distinct()

        val languages = contentLanguage.map { langName =>
            DataNode(langName, Option(Map("name" -> langName)), Option(List(NODE_NAME)));
        }
        ppQueries.union(sc.parallelize(Seq(GraphDBUtil.createNodesQuery(languages)) ++ algorithmQueries, JobContext.parallelization));
    }

}