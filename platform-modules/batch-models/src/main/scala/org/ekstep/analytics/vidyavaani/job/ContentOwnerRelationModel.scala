package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.framework.IJob
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.neo4j.spark._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.util.Neo4jUtil
import org.ekstep.analytics.util.Node
import org.ekstep.analytics.util.Neo4jQueryConstants._

object ContentOwnerRelationModel extends optional.Application with IJob {

    def main(config: String)(implicit sc: Option[SparkContext] = None) {

        val jobConfig = JSONUtils.deserialize[JobConfig](config);

        if (null == sc.getOrElse(null)) {
            JobContext.parallelization = 10;
            implicit val sparkContext = CommonUtil.getSparkContext(JobContext.parallelization, jobConfig.appName.getOrElse("Vidyavaani Neo4j Model"));
            try {
                execute();
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

        implicit val neo = Neo4j(sc)
        val oNode = Node("Owner")
        val dNode = Node("domain", Option(Map("IL_FUNC_OBJECT_TYPE"->"Content")))
        Neo4jUtil.deleteNodes(oNode)
        _createOwnerNode(dNode, oNode)
        _createRelation()
    }

    private def _createOwnerNode(dNode: Node, oNode: Node)(implicit neo: Neo4j) = {

        // Query to fetch all node having Owner properties
        //val props = Neo4jUtil.removeKeyQuotes(JSONUtils.serialize(dNode.properties.getOrElse(Map())));
        
//        val searchQuery = StringBuilder.newBuilder;
//        searchQuery.append(MATCH).append(OPEN_COMMON_BRACKETS_WITH_NODE_OBJECT_VARIABLE).append(dNode.label).append(props).append(CLOSE_COMMON_BRACKETS)
//        .append(BLANK_SPACE).append(WHERE).append(DEFAULT_CYPHER_NODE_OBJECT).append(DOT)
        
        val query = """Match (n:domain) where n.IL_FUNC_OBJECT_TYPE='Content' AND EXISTS(n.owner) AND EXISTS(n.portalOwner) return DISTINCT n.owner, n.portalOwner"""

        val ownerList = neo.cypher(query).loadRowRdd.map { x => (x.get(1).toString(), x.get(0).toString()) }.groupBy(f => f._1)
            .map { f =>
                val id = f._2.map(f => f._2).filter { x => !"".equals(x) }.toArray
                (f._1, id)
            }.map { x => if (x._2.length == 0) (x._1, x._1) else (x._1, x._2.last) }.collect
        
            
        for (o <- ownerList) {
//            val id = o._1
//            val owner = o._2
            Neo4jUtil.createNode(Node(oNode.label,Option(Map("IL_UNIQUE_ID" -> o._1, "name"-> o._2))))
            //val script = s"CREATE (o:Owner {name: '${owner.replace("'", "")}', IL_UNIQUE_ID: '${id}'})"
            //neo.cypher(script).loadRowRdd.collect
        }
    }

    private def _createRelation()(implicit neo: Neo4j) = {
        val script = """MATCH (a:domain), (b:Owner) WHERE a.IL_FUNC_OBJECT_TYPE = 'Content' AND EXISTS(a.portalOwner) AND a.portalOwner = b.IL_UNIQUE_ID CREATE (a)-[r:createdBy]->(b)"""
        neo.cypher(script).loadRowRdd.collect
    }
}