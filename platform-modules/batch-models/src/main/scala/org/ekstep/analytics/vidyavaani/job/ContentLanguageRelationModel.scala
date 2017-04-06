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

    override def preProcess(input: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {
        val job_config = sc.cassandraTable[Job_Config](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_CONFIG).where("category='vv' AND config_key=?", "content-lang-rel").first
        val queries = job_config.config_value.get("cleanupQueries").get ++ job_config.config_value.get("algorithmQueries").get
        sc.parallelize(queries, JobContext.parallelization);
    }

    override def algorithm(ppQueries: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {
        ppQueries;
    }

}