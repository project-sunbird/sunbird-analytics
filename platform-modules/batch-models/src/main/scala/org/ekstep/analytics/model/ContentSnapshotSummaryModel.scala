package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.joda.time.DateTime
import org.ekstep.analytics.util.CypherQueries

case class ContentSnapshotAlgoOutput(author_id: String, partner_id: String, total_user_count: Long, active_user_count: Long, total_content_count: Long, live_content_count: Long, review_content_count: Long) extends AlgoOutput

object ContentSnapshotSummaryModel extends IBatchModelTemplate[Empty, Empty, ContentSnapshotAlgoOutput, MeasuredEvent] with Serializable {

    override def name(): String = "ContentSnapshotSummaryModel";
    implicit val className = "org.ekstep.analytics.model.ContentSnapshotSummaryModel";
    
    override def preProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {

        sc.makeRDD(List(Empty()));
    }

    override def algorithm(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentSnapshotAlgoOutput] = {

        val graphDBConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
            "user" -> AppConf.getConfig("neo4j.bolt.user"),
            "password" -> AppConf.getConfig("neo4j.bolt.password"));

        val active_user_days_limit = config.getOrElse("active_user_days_limit", 30).asInstanceOf[Int];
        val days_limit_timestamp = new DateTime().minusDays(active_user_days_limit).getMillis
        
        // for author_id = all
        val total_user_count = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_SNAPSHOT_TOTAL_USER_COUNT).list().get(0).get("count(usr)").asLong();
        val active_users = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_SNAPSHOT_ACTIVE_USER_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map{ x =>
            val ts = CommonUtil.getTimestamp(x.get("cnt.createdOn").asString(), CommonUtil.df5, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            (x.get("usr.IL_UNIQUE_ID").asString(), ts)
        }
        val active_user_count = sc.parallelize(active_users).filter(f => f._2 >= days_limit_timestamp).count()
        val total_content_count = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_SNAPSHOT_TOTAL_CONTENT_COUNT).list().get(0).get("count(cnt)").asLong()
        val review_content_count = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_SNAPSHOT_REVIEW_CONTENT_COUNT).list().get(0).get("count(cnt)").asLong()
        val live_content_count = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_SNAPSHOT_LIVE_CONTENT_COUNT).list().get(0).get("count(cnt)").asLong()
        val rdd1 = sc.makeRDD(List(ContentSnapshotAlgoOutput("all", "all", total_user_count, active_user_count, total_content_count, live_content_count, review_content_count)))
        
        // for specific author_id
        val author_total_content_count = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_SNAPSHOT_AUTHOR_TOTAL_CONTENT_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map{x => (x.get("usr.IL_UNIQUE_ID").asString(), x.get("usr.contentCount").asLong())}
        val author_live_content_count = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_SNAPSHOT_AUTHOR_LIVE_CONTENT_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map{x => (x.get("usr.IL_UNIQUE_ID").asString(), x.get("usr.liveContentCount").asLong())}
        val author_review_content_count = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_SNAPSHOT_AUTHOR_REVIEW_CONTENT_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map{x => (x.get("usr.IL_UNIQUE_ID").asString(), x.get("rcc").asLong())}
        
        val totalContentRDD = sc.parallelize(author_total_content_count)
        val liveContentRDD = sc.parallelize(author_live_content_count)
        val reviewContentRDD = sc.parallelize(author_review_content_count)
        val rdd2 = totalContentRDD.leftOuterJoin(liveContentRDD).leftOuterJoin(reviewContentRDD).map{f =>
            ContentSnapshotAlgoOutput(f._1, "all", 0, 0, f._2._1._1, f._2._1._2.getOrElse(0), f._2._2.getOrElse(0))
        }
        
        rdd1++(rdd2);
    }

    override def postProcess(data: RDD[ContentSnapshotAlgoOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {

        data.map { x =>
            val mid = CommonUtil.getMessageId("ME_CONTENT_SNAPSHOT_SUMMARY", x.author_id, "SNAPSHOT", DtRange(System.currentTimeMillis(), System.currentTimeMillis()));
            val measures = Map(
                "total_user_count" -> x.total_user_count,
                "active_user_count" -> x.active_user_count,
                "total_content_count" -> x.total_content_count,
                "live_content_count" -> x.live_content_count,
                "review_content_count" -> x.review_content_count);
            MeasuredEvent("ME_CONTENT_SNAPSHOT_SUMMARY", System.currentTimeMillis(), System.currentTimeMillis(), "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ContentSnapshotSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "SNAPSHOT").asInstanceOf[String], DtRange(System.currentTimeMillis(), System.currentTimeMillis())),
                Dimensions(None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, Option(x.author_id), Option(x.partner_id)),
                MEEdata(measures));
        }
    }

}