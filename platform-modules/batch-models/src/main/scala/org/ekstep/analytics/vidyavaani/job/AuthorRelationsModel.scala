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

object AuthorRelationsModel extends optional.Application with IJob {

    val NODE_NAME = "User";
    val CONTENT_AUTHOR_RELATION = "createdBy"
    val AUTHOR_CONCEPT_RELATION = "uses"
    
    val INDEX_QUERY = "CREATE INDEX ON :User(type)"
    val AUTHOR_CONCEPT_REL_QUERY = "MATCH (A:User {type:'author'}), (C:domain{IL_FUNC_OBJECT_TYPE:'Concept'}) OPTIONAL MATCH path = (C)<-[:associatedTo]-(f:domain{IL_FUNC_OBJECT_TYPE:'Content',status:'Live'})-[:createdBy]->(A) WITH A, C, CASE WHEN path is null THEN 0 ELSE COUNT(path) END AS overlap MERGE (C)-[:usedBy{support:overlap}]->(A)"
    val AUTHOR_CONCEPT_REL_DEL_QUERY = "MATCH (n: domain{IL_FUNC_OBJECT_TYPE:'Concept'})-[r:usedBy]->(m: User{type:'author'}) delete r;"
    
    implicit val className = "org.ekstep.analytics.vidyavaani.job.AuthorRelationsModel"

    def main(config: String)(implicit sc: Option[SparkContext] = None) {

        JobLogger.init("AuthorRelationsModel")
        JobLogger.start("AuthorRelationsModel Started executing", Option(Map("config" -> config)))

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
            _createAuthorNodeWithRelation();
            _createAuthorConceptRelation();
        })

        JobLogger.end("AuthorRelationsModel Completed", "SUCCESS", Option(Map("date" -> "", "inputEvents" -> 0, "outputEvents" -> 0, "timeTaken" -> time._1)));
    }

    private def _createAuthorNodeWithRelation()(implicit sc: SparkContext) = {
        val limit = if (StringUtils.isNotBlank(AppConf.getConfig("graph.content.limit")))
            Option(Integer.parseInt(AppConf.getConfig("graph.content.limit"))) else None

        val contentNodes = GraphDBUtil.findNodes(Map("IL_FUNC_OBJECT_TYPE" -> "Content"), Option(List("domain")), limit);

        val owners = contentNodes.map { x => x.metadata.getOrElse(Map()) }
            .map(f => (f.getOrElse("portalOwner", "").asInstanceOf[String], f.getOrElse("owner", "").asInstanceOf[String]))
            .groupBy(f => f._1).filter(p => !StringUtils.isBlank(p._1))
            .map { f =>
                val identifier = f._1;
                val namesList = f._2.filter(p => !StringUtils.isBlank(p._2));
                val name = if (namesList.isEmpty) identifier else namesList.last._2;
                DataNode(identifier, Option(Map("name" -> name, "type" -> "author")), Option(List(NODE_NAME)));
            }

        GraphDBUtil.createNodes(owners);

        val ownerContentRels = contentNodes.map { x => x.metadata.getOrElse(Map()) }
            .map(f => (f.getOrElse("portalOwner", "").asInstanceOf[String], f.getOrElse("IL_UNIQUE_ID", "").asInstanceOf[String]))
            .filter(f => StringUtils.isNoneBlank(f._1) && StringUtils.isNoneBlank(f._2))
            .map { f =>
                val startNode = DataNode(f._1, None, Option(List("User")));
                val endNode = DataNode(f._2, None, Option(List("domain")));
                Relation(startNode, endNode, CONTENT_AUTHOR_RELATION, RelationshipDirection.INCOMING.toString);
            };
        GraphDBUtil.addRelations(ownerContentRels);

    }

    def _createAuthorConceptRelation()(implicit sc: SparkContext) = {
        
        val config = Map("url" -> AppConf.getConfig("neo4j.bolt.url"), "user" -> AppConf.getConfig("neo4j.bolt.user"), "password" -> AppConf.getConfig("neo4j.bolt.password"));
        
        // Deleteing usedBy relation between Concept -> Author
        GraphQueryDispatcher.dispatch(config, AUTHOR_CONCEPT_REL_DEL_QUERY)
        
        // Indexing Author node
        GraphQueryDispatcher.dispatch(config, INDEX_QUERY)
        
        // Adding relation between Concept -> Author
        GraphQueryDispatcher.dispatch(config, AUTHOR_CONCEPT_REL_QUERY)       
    }
}