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
    
    val getUniqueAuthorsQuery = "MATCH (n:domain) WHERE (n.createdBy IS NOT null AND NOT n.createdBy = '') WITH n.createdBy AS author, CASE WHEN  last(collect(n.creator))IS null THEN n.createdBy ELSE last(collect(n.creator)) END AS name, CASE WHEN  last(collect(n.appId))IS null THEN 'NA' ELSE last(collect(n.appId)) END AS appId, CASE WHEN  last(collect(n.channel))IS null THEN 'in.ekstep' ELSE last(collect(n.channel)) END AS channel  RETURN author, name, appId, channel"
    
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

        val authors = GraphQueryDispatcher.dispatch(getUniqueAuthorsQuery).list().toArray();
        val authorsRDD = authors.map(x => x.asInstanceOf[org.neo4j.driver.v1.Record]).map{x => (x.get("author").asString(), x.get("name").asString(), x.get("appId").asString(), x.get("channel").asString())}.map{ f =>
            DataNode(f._1, Option(Map("name" -> f._2, "type" -> "author", "appId" -> f._3, "channel" -> f._4)), Option(List(NODE_NAME)));
        }
        val authorQuery = GraphDBUtil.createNodesQuery(sc.parallelize(authorsRDD))
        ppQueries.union(sc.parallelize(Seq(authorQuery) ++ algorithmQueries, JobContext.parallelization));
    }
}