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

case class AlgorithmInput(cleanupQueries: List[String], algorithmQueries: List[String])

object AuthorRelationsModel extends IGraphExecutionModel with Serializable {

    val NODE_NAME = "User";
    val CONTENT_AUTHOR_RELATION = "createdBy";
    
//    val optimizationQueries = Seq("CREATE INDEX ON :User(type)");

    // Cleanup Queries:
//    val cleanupQueries = Seq("MATCH (usr :User {type:'author'})<-[r:createdBy]-(cnt :domain{IL_FUNC_OBJECT_TYPE:'Content'}) DELETE r", // To delete Content-createdBy->User:author relation.
//    					"MATCH (usr :User {type:'author'})-[r:uses]->(cnc :domain{IL_FUNC_OBJECT_TYPE:'Concept'}) DELETE r", // To delete User-uses->Concept relation.
//    					"MATCH (usr:User {type:'author'})-[r:createdIn]->(lan:Language) DELETE r", // To delete User-createdIn->Language relation.
//    					"MATCH(ee:User{type: 'author'}) DETACH DELETE ee");  // To delete user nodes along with its relations.
    
    
    // Algorithm Queries
    val algorithmQueries = Seq("MATCH (usr:User{type:'author'}), (cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE usr.IL_UNIQUE_ID = cnt.portalOwner MERGE (cnt)-[r:createdBy]->(usr)", // To create Content-createBy->User:author relation.
    					"MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Draft', 'Review', 'Live'] WITH usr,count(cnt) as cc SET usr.contentCount = cc", // To set contentCount property value on User:author node.
    					"MATCH (usr:User {type:'author'})<-[r:createdBy]-(cnt: domain{IL_FUNC_OBJECT_TYPE:'Content'}) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Live'] WITH usr,count(cnt) as cc SET usr.liveContentCount = cc", // To set liveContentCount property value on User:author node.
    					"MATCH (usr:User {type:'author'}), (cnc:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) MERGE (usr)-[r:uses{contentCount:0, liveContentCount:0}]->(cnc)", // To create User:author-uses->Concept relation with default property values.
    					"MATCH (usr:User {type:'author'}), (cnc:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) MATCH path = (usr)<-[:createdBy]-(cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'})-[nc:associatedTo]->(cnc), (usr)-[r:uses]->(cnc) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Draft', 'Review', 'Live'] WITH r, count(cnt) AS cc SET r.contentCount=cc", // To set contentCount property value on User:author-uses->Concept relation. 
    					"MATCH (usr:User {type:'author'}), (cnc:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) MATCH path = (usr)<-[:createdBy]-(cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'})-[nc:associatedTo]->(cnc), (usr)-[r:uses]->(cnc) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Live'] WITH r, count(cnt) AS cc SET r.liveContentCount=cc",  // To set liveContentCount property value on User:author-uses->Concept relation.
    					"MATCH p=(usr: User {type:'author'})-[r:uses]-(cnc: domain{IL_FUNC_OBJECT_TYPE:'Concept'}) WHERE r.contentCount > 0 WITH usr, count(p) AS cncCount SET usr.conceptsCount=cncCount", // To set conceptCount property value on User:author.
    					"MATCH (usr :User {type:'author'}), (lan:Language) MERGE (usr)-[r:createdIn{contentCount:0, liveContentCount:0}]->(lan)", // To create User:author-createdIn->Language relation with default property values.
    					"MATCH (usr:User {type:'author'}), (lan:Language), (cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'}), (usr)-[r:createdIn]->(lan) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Draft', 'Review', 'Live'] AND cnt.portalOwner = usr.IL_UNIQUE_ID AND lan.IL_UNIQUE_ID IN extract(language IN cnt.language | lower(language)) WITH r, count(cnt) AS cc SET r.contentCount=cc", // To set contentCount property value on User:author-createdIn->Language relation.
    					"MATCH (usr:User {type:'author'}), (lan:Language), (cnt:domain{IL_FUNC_OBJECT_TYPE:'Content'}), (usr)-[r:createdIn]->(lan) WHERE lower(cnt.contentType) IN ['story', 'game', 'collection', 'worksheet'] AND cnt.status IN ['Live'] AND cnt.portalOwner = usr.IL_UNIQUE_ID AND lan.IL_UNIQUE_ID IN extract(language IN cnt.language | lower(language)) WITH r, count(cnt) AS lcc SET r.liveContentCount=lcc"); // To set liveContentCount property value on User:author-createdIn->Language relation.
    
    override def name(): String = "ContentLanguageRelationModel";
    override implicit val className = "org.ekstep.analytics.vidyavaani.job.AuthorRelationsModel"

    override def preProcess(input: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {
        val job_config = sc.cassandraTable[Job_Config](Constants.PLATFORM_KEY_SPACE_NAME, Constants.JOB_CONFIG).where("category='vv' AND config_key=?", "content-own-rel").first//VVJobsConfig("content-own-rel", optimizationQueries.toList, cleanupQueries.toList, algorithmQueries.toList)
        val optimizationQueries = job_config.config_value.get("optimizationQueries").get
        val cleanupQueries = job_config.config_value.get("cleanupQueries").get
        val algorithmQueries = job_config.config_value.get("algorithmQueries").get
        executeQueries(sc.parallelize(optimizationQueries, JobContext.parallelization));
        sc.parallelize(cleanupQueries, JobContext.parallelization);
    }

    override def algorithm(ppQueries: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {

        val contentNodes = GraphDBUtil.findNodes(Map("IL_FUNC_OBJECT_TYPE" -> "Content"), Option(List("domain")));

        val authorNodes = contentNodes.map { x => x.metadata.getOrElse(Map()) }
            .map(f => (f.getOrElse("portalOwner", "").asInstanceOf[String], f.getOrElse("owner", "").asInstanceOf[String]))
            .groupBy(f => f._1).filter(p => !StringUtils.isBlank(p._1))
            .map { f =>
                val identifier = f._1;
                val namesList = f._2.filter(p => !StringUtils.isBlank(p._2));
                val name = if (namesList.isEmpty) identifier else namesList.last._2;
                DataNode(identifier, Option(Map("name" -> name, "type" -> "author")), Option(List(NODE_NAME)));
            }

        val authorQuery = GraphDBUtil.createNodesQuery(authorNodes)
        ppQueries.union(sc.parallelize(Seq(authorQuery) ++ algorithmQueries, JobContext.parallelization));
    }
}