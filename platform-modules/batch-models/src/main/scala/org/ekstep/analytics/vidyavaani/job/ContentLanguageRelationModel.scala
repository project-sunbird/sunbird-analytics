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
	
	val deleteQuery = "MATCH (lan:Language{}) DETACH DELETE lan";
	val findQuery = "MATCH (cnt:domain{IL_FUNC_OBJECT_TYPE: 'Content'}) WHERE lower(cnt.contentType)IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Draft', 'Review', 'Live'] RETURN cnt.language"
	val relationQuery = "MATCH (cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'}), (lan:Language{}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Draft', 'Review', 'Live'] AND lan.IL_UNIQUE_ID IN extract(language IN cnt.language | lower(language)) CREATE (cnt)-[r:expressedIn]->(lan) RETURN r";
	val contentCountUpdateQuery = "MATCH (cnt:domain{IL_FUNC_OBJECT_TYPE: 'Content'}), (lan:Language{}) WHERE lower(cnt.contentType)IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Draft', 'Review', 'Live'] MATCH p=(cnt)-[r:expressedIn]->(lan) WITH lan, COUNT(p) AS cc SET lan.contentCount = cc"
	val liveContentCountUpdateQuery = "MATCH (cnt:domain{IL_FUNC_OBJECT_TYPE: 'Content'}), (lan:Language{}) WHERE lower(cnt.contentType)IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Live'] OPTIONAL MATCH p=(cnt)-[r:expressedIn]->(lan) WITH lan, COUNT(p) AS lcc SET lan.liveContentCount = lcc"
	
	override def preProcess(input: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {
		sc.parallelize(Seq(deleteQuery), JobContext.parallelization);
	}

	override def algorithm(ppQueries: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {

		val contentNodes = GraphQueryDispatcher.dispatch(graphDBConfig, findQuery);
		val res = contentNodes.list().map { x => (x.get("cnt.language", new java.util.ArrayList()).asInstanceOf[java.util.List[String]]) }
			.flatMap(f => f).filter(f => StringUtils.isNoneBlank(f))
		val contentLanguage = sc.parallelize(res).distinct()

		val languages = contentLanguage.map { f =>
				DataNode(f.toLowerCase(), Option(Map("name" -> f)), Option(List(NODE_NAME)));
		}
		ppQueries.union(sc.parallelize(Seq(GraphDBUtil.createNodesQuery(languages), relationQuery, contentCountUpdateQuery, liveContentCountUpdateQuery), JobContext.parallelization));
	}

}