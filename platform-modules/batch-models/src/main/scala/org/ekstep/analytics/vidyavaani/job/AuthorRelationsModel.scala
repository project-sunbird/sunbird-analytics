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
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.job.IGraphExecutionModel
import org.apache.spark.rdd.RDD
import scala.collection.JavaConversions._
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.Job_Config
import org.ekstep.analytics.util.Constants

/**
*
* Creates User Node.
* Creates relation between User and Content (createdBy).
* Add Property contentCount, liveContentCount on User Node.
* Creates relation between User and Concept (uses).
* Add Property contentCount, liveContentCount on uses relation between User and Concept.
* Add Property conceptCount on User Node.
* Creates relation between User and Language (createdIn).
* Add Property contentCount, liveContentCount on createdIn relation between User and Language.
* Creates ContentType Nodes (Worksheet, Story, Game, Collection)
* Creates relation between User and ContentType (uses).
* Add Property contentCount, liveContentCount on uses relation between User and ContentType.
* Add relation between Content and ContentType (isA).
* Add Property contentCount, liveContentCount on ContentType Node.
*/

object AuthorRelationsModel extends IGraphExecutionModel with Serializable {

    val NODE_NAME = "User";
    val CONTENT_AUTHOR_RELATION = "createdBy";
    var algorithmQueries: List[String] = List();
    
    override def name(): String = "AuthorRelationsModel";
    override implicit val className = "org.ekstep.analytics.vidyavaani.job.AuthorRelationsModel"

    override def preProcess(input: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {
        val job_config = sc.cassandraTable[Job_Config](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_CONFIG).where("category='vv' AND config_key=?", "content-own-rel").first
        val optimizationQueries = job_config.config_value.get("optimizationQueries").get
        val cleanupQueries = job_config.config_value.get("cleanupQueries").get
        algorithmQueries = job_config.config_value.get("algorithmQueries").get
        executeQueries(sc.parallelize(optimizationQueries, JobContext.parallelization));
        sc.parallelize(cleanupQueries, JobContext.parallelization);
    }

    override def algorithm(ppQueries: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {

        val contentNodes = GraphDBUtil.findNodes(Map("IL_FUNC_OBJECT_TYPE" -> "Content"), Option(List("domain")));

        val authorNodes = contentNodes.map { x => x.metadata.getOrElse(Map()) }
            .map(f => (f.getOrElse("createdBy", "").asInstanceOf[String], f.getOrElse("creator", "").asInstanceOf[String], f.getOrElse("appId", "").asInstanceOf[String], f.getOrElse("channel", "in.ekstep").asInstanceOf[String]))
            .groupBy(f => f._1).filter(p => !StringUtils.isBlank(p._1))
            .map { f =>
                val identifier = f._1;
                val namesList = f._2.filter(p => !StringUtils.isBlank(p._2));
                val name = if (namesList.isEmpty) identifier else namesList.last._2;
                val defaultAppId = AppConf.getConfig("default.creation.app.id");
                val defaultChannel = AppConf.getConfig("default.channel.id");
                val appId = if (namesList.isEmpty) defaultAppId else if (StringUtils.isBlank(namesList.last._3)) defaultAppId else namesList.last._3;
                val channel = if (namesList.isEmpty) defaultChannel else if (StringUtils.isBlank(namesList.last._4)) defaultChannel else namesList.last._4;
                DataNode(identifier, Option(Map("name" -> name, "type" -> "author", "appId" -> appId, "channel" -> channel)), Option(List(NODE_NAME)));
            }

        val authorQuery = GraphDBUtil.createNodesQuery(authorNodes)
        ppQueries.union(sc.parallelize(Seq(authorQuery) ++ algorithmQueries, JobContext.parallelization));
    }
}