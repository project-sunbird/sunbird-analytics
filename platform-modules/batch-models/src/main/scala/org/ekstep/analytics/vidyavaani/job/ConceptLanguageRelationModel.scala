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
import org.ekstep.analytics.job.IGraphExecutionModel
import org.apache.spark.rdd.RDD

object ConceptLanguageRelationModel extends IGraphExecutionModel with Serializable {

    override def name(): String = "ConceptLanguageRelationModel";
    override implicit val className = "org.ekstep.analytics.vidyavaani.job.ConceptLanguageRelationModel"
    
    val RELATION = "usedIn";
    
    val relationQuery = "MATCH (l:Language), (c:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) OPTIONAL MATCH p=(l)<-[ln:expressedIn]-(n:domain{IL_FUNC_OBJECT_TYPE:'Content'})-[nc:associatedTo]->(c) WHERE lower(n.contentType) IN ['story', 'game', 'collection', 'worksheet'] WITH c, l, CASE WHEN p is null THEN 0 ELSE COUNT(p) END AS cc MERGE (c)-[r:usedIn{contentCount: cc}]->(l) RETURN r"
    val deleteRelQuery = "MATCH (c:domain{IL_FUNC_OBJECT_TYPE:'Concept'})-[r:usedIn]->(l:Language) DELETE r"

    override def preProcess(input: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {
        sc.parallelize(Seq(deleteRelQuery), JobContext.parallelization);
    }

    override def algorithm(ppQueries: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {
        ppQueries.union(sc.parallelize(Seq(relationQuery), JobContext.parallelization));
    }

}