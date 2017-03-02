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

case class ContentData(content_id: String, body: Array[Byte], last_updated_on: DateTime, oldbody: Option[Array[Byte]]);

object ContentAssetRelationModel extends optional.Application with IJob {

    val RELATION = "uses";

    def main(config: String)(implicit sc: Option[SparkContext] = None) {

        val jobConfig = JSONUtils.deserialize[JobConfig](config);

        if (null == sc.getOrElse(null)) {
            JobContext.parallelization = 10;
            implicit val sparkContext = CommonUtil.getSparkContext(JobContext.parallelization, jobConfig.appName.getOrElse("Vidyavaani Neo4j Model"));
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
        val data = sc.cassandraTable[ContentData](Constants.CONTENT_STORE_KEY_SPACE_NAME, Constants.CONTENT_DATA_TABLE).map { x => (x.content_id, getAssetIds(new String(x.body, "UTF-8"))) }
            .filter { x => x._2.nonEmpty }.map { x =>
                val startNode = DataNode(x._1, None, Option(List("domain")));
                x._2.map { x =>
                    val endNode = DataNode(x, None, Option(List("domain")));
                    Relation(startNode, endNode, RELATION, RelationshipDirection.OUTGOING.toString);
                }
            }.flatMap { x => x }

        GraphDBUtil.addRelations(data);
    }

    private def getAssetIds(body: String): Array[String] = {

        if (body.startsWith("<")) {
            val dom = XML.loadString(body)
            val els = dom \ "manifest" \ "media"

            val assestIds = els.map { x =>
                val node = x.attribute("asset_id").getOrElse(null)
                if (node != null)
                    node.text
                else "";
            }.filter { x => !"".equals(x) }.toArray
            assestIds;
        } else {
            val mediaList = JSONUtils.deserialize[Map[String, Map[String, AnyRef]]](body).get("theme").get.get("manifest").get.asInstanceOf[Map[String, AnyRef]].get("media").get.asInstanceOf[List[Map[String, AnyRef]]];
            mediaList.map { x =>
                x.getOrElse("assetId", "").asInstanceOf[String]
            }.filter { x => !"".equals(x) }.toArray
        }

    }
}