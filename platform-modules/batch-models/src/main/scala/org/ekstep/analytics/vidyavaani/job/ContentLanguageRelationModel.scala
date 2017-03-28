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


object ContentLanguageRelationModel extends IGraphExecutionModel with Serializable {

	override def name(): String = "ContentLanguageRelationModel";
	override implicit val className = "org.ekstep.analytics.vidyavaani.job.ContentLanguageRelationModel";
	
	val NODE_NAME = "Language";
	val RELATION = "expressedIn";
	

	val findQuery = "MATCH (n:domain) where n.IL_FUNC_OBJECT_TYPE = 'Content' AND n.contentType IN ['Story', 'Game', 'Collection', 'Worksheet'] return n.language, n.IL_UNIQUE_ID"
	val relationQuery = "MATCH (n:domain{IL_FUNC_OBJECT_TYPE:'Content'}), (l:Language{}) WHERE lower(n.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND l.IL_UNIQUE_ID IN extract(language IN n.language | lower(language)) CREATE (n)-[r:expressedIn]->(l) RETURN r"
	val deleteQuery = "MATCH (l:Language{}) DETACH DELETE l"

	
	override def preProcess(input: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {
		sc.parallelize(Seq(deleteQuery), JobContext.parallelization);
	}

	override def algorithm(ppQueries: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {

		val contentNodes = GraphQueryDispatcher.dispatch(graphDBConfig, findQuery);
		val res = contentNodes.list().map { x => (x.get("n.language", new java.util.ArrayList()).asInstanceOf[java.util.List[String]], x.get("n.IL_UNIQUE_ID", "").asInstanceOf[String]) }
			.map(f => for (i <- f._1) yield (i.toString().toLowerCase(), f._2)).flatMap(f => f)
			.filter(f => StringUtils.isNoneBlank(f._1) && StringUtils.isNoneBlank(f._2))
		val contentLanguage = sc.parallelize(res)

		val languages = contentLanguage.groupByKey().map(f => (f._1, f._2.size))
			.map { f =>
				DataNode(f._1.toLowerCase(), Option(Map("name" -> f._1, "contentCount" -> f._2.asInstanceOf[AnyRef])), Option(List(NODE_NAME)));
			}
			
		ppQueries.union(sc.parallelize(Seq(GraphDBUtil.createNodesQuery(languages), relationQuery), JobContext.parallelization));
	}

}