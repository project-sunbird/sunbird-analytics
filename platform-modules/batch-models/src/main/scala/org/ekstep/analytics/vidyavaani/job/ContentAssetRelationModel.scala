package org.ekstep.analytics.vidyavaani.job

import org.ekstep.analytics.framework.IJob
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.neo4j.driver.v1.Session
import com.datastax.spark.connector._
import org.joda.time.DateTime
import scala.xml.XML
import org.ekstep.analytics.framework.Relation
import org.ekstep.analytics.framework.DataNode
import org.ekstep.analytics.framework.RelationshipDirection
import org.ekstep.analytics.framework.util.GraphDBUtil
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.JobLogger
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.ECMLUtil
import org.ekstep.analytics.framework.UpdateDataNode
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.job.IGraphExecutionModel

case class ContentData(content_id: String, body: Option[Array[Byte]], last_updated_on: Option[DateTime], oldbody: Option[Array[Byte]]);

object ContentAssetRelationModel extends IGraphExecutionModel with Serializable {

    val RELATION = "uses";
    override implicit val className = "org.ekstep.analytics.vidyavaani.job.ContentAssetRelationModel"

    val deleteRelQuery = "MATCH (n1: domain) - [r: uses] -> (n2: domain) where n2.contentType = 'Asset' DELETE r"
    val updateAssetNodeQuery = "MATCH (a:domain{IL_FUNC_OBJECT_TYPE:'Content', contentType:'Asset'}), (c:domain{IL_FUNC_OBJECT_TYPE:'Content'}) MATCH p=(a)<-[r:uses]-(c) WHERE lower(c.contentType) IN ['story', 'game', 'collection', 'worksheet']  WITH a, COUNT(p) AS cc SET a.contentCount = cc"

    override def preProcess(input: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {
        sc.parallelize(Seq(deleteRelQuery), JobContext.parallelization);
    }

    override def algorithm(ppQueries: RDD[String], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[String] = {
        val data = sc.cassandraTable[ContentData](Constants.CONTENT_STORE_KEY_SPACE_NAME, Constants.CONTENT_DATA_TABLE)
            .map { x => (x.content_id, new String(x.body.getOrElse(Array()), "UTF-8")) }.filter { x => !x._2.isEmpty }
            .map(f => (f._1, getAssetIds(f._2, f._1))).filter { x => x._2.nonEmpty }

        val relationsData = data.map { x =>
            val startNode = DataNode(x._1, None, Option(List("domain")));
            x._2.map { x =>
                val endNode = DataNode(x, None, Option(List("domain")));
                GraphDBUtil.addRelationQuery(startNode, endNode, RELATION, RelationshipDirection.OUTGOING.toString)
            }
        }.flatMap { x => x }
        ppQueries.union(relationsData).union(sc.parallelize(Seq(updateAssetNodeQuery), JobContext.parallelization));
    }

    private def getAssetIds(body: String, contentId: String): List[String] = {
        try {
            if (body.startsWith("<")) {
                val dom = XML.loadString(body)
                val els = dom \ "manifest" \ "media"

                val assestIds = els.map { x =>
                    val node = x.attribute("asset_id").getOrElse(x.attribute("assetId").getOrElse(null))
                    if (node != null)
                        node.text
                    else "";
                }.filter { x => StringUtils.isNotBlank(x) }.toList
                assestIds;
            } else {
                ECMLUtil.getAssetIds(body);
            }
        } catch {
            case t: Throwable =>
                println("Unable to parse OR fetch Asset Ids for contentId:" + contentId + " :: Error" + t.getMessage);
                List();
        }
    }
}