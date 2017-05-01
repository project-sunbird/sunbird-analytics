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
import scala.collection.JavaConversions._
import org.apache.commons.lang3.StringUtils

case class ContentSnapshotAlgoOutput(total_content_count: Long, live_content_count: Long, review_content_count: Long, total_user_count: Long = 0L, active_user_count: Long = 0L, author_id: String = "all", partner_id: String = "all") extends AlgoOutput

object ContentSnapshotSummaryModel extends IBatchModelTemplate[DerivedEvent, DerivedEvent, ContentSnapshotAlgoOutput, MeasuredEvent] with Serializable {

    override def name(): String = "ContentSnapshotSummaryModel";
    implicit val className = "org.ekstep.analytics.model.ContentSnapshotSummaryModel";
    val graphDBConfig = Map("url" -> AppConf.getConfig("neo4j.bolt.url"),
            "user" -> AppConf.getConfig("neo4j.bolt.user"),
            "password" -> AppConf.getConfig("neo4j.bolt.password"));
    
    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        data;
    }
    
    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentSnapshotAlgoOutput] = {

        val active_user_days_limit = config.getOrElse("active_user_days_limit", 30).asInstanceOf[Int];
        val days_limit_timestamp = new DateTime().minusDays(active_user_days_limit).getMillis
        
        // For author_id = all
        val total_user_count = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_SNAPSHOT_TOTAL_USER_COUNT).list().get(0).get("count(usr)").asLong();
        val active_users = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_SNAPSHOT_ACTIVE_USER_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map{ x =>
            val ts = CommonUtil.getTimestamp(x.get("cnt.createdOn").asString(), CommonUtil.df5, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            (x.get("usr.IL_UNIQUE_ID").asString(), ts)
        }
        val active_user_count = sc.parallelize(active_users).filter(f => f._2 >= days_limit_timestamp).map(x => x._1).distinct().count();
        
        val contentCountByStatus = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_COUNT_BY_STATUS).list()
        	.toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
        	.map { x => (x.get("status").asString(), x.get("count").asLong()) }.toMap;
        	
        val totalContentCount = contentCountByStatus.values.reduce((a,b) => a + b);
        val liveContentCount = contentCountByStatus.getOrElse("live", 0).asInstanceOf[Number].longValue();
        val reviewContentCount = contentCountByStatus.getOrElse("review", 0).asInstanceOf[Number].longValue();
        val rdd1 = sc.makeRDD(List(ContentSnapshotAlgoOutput(totalContentCount, liveContentCount, reviewContentCount, total_user_count, active_user_count)))
        
        // For specific author_id
        val contentCountPerAuthorByStatus = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_COUNT_PER_AUTHOR_BY_STATUS).list()
        	.toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
        	.map { x => (x.get("identifier").asString(), x.get("status").asString(), x.get("count").asLong()) }.groupBy(f => f._1);

       val rdd2 = sc.makeRDD(contentCountPerAuthorByStatus.map { f =>
			val total = f._2.map(f => f._3).reduce((a,b) => a+b);
    	    val live = f._2.filter(f => StringUtils.equals("live", f._2)).headOption.getOrElse((f._1, "live", 0L))._3;
    	    val review = f._2.filter(f => StringUtils.equals("review", f._2)).headOption.getOrElse((f._1, "review", 0L))._3;
    	    ContentSnapshotAlgoOutput(total, live, review, 0, 0, f._1);
       }.toList)
        
        // For specific partner_id
        val partner_user = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_SNAPSHOT_PARTNER_USER_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map{x => (x.get("usr.IL_UNIQUE_ID").asString(), x.get("cnt.createdFor").asList().toList, x.get("cnt.createdOn").asString())}
        val partner_active_users = partner_user.map{ x =>
            val ts = CommonUtil.getTimestamp(x._3, CommonUtil.df5, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            val partners = x._2.map { x => x.toString()}
            (for(i <- partners) yield (i, x._1, ts))
        }.flatMap(f => f)
        
        val partner_total_content_count = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_SNAPSHOT_PARTNER_TOTAL_CONTENT_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map{x => (x.get("cnt.IL_UNIQUE_ID").asString(), x.get("cnt.createdFor").asList().toList)}
        val partner_live_content_count = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_SNAPSHOT_PARTNER_LIVE_CONTENT_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map{x => (x.get("cnt.IL_UNIQUE_ID").asString(), x.get("cnt.createdFor").asList().toList)}
        val partner_review_content_count = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_SNAPSHOT_PARTNER_REVIEW_CONTENT_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map{x => (x.get("cnt.IL_UNIQUE_ID").asString(), x.get("cnt.createdFor").asList().toList)}
        
        val partnerActiveUserCountRDD = sc.parallelize(partner_active_users).filter(f => f._3 >= days_limit_timestamp).groupBy(f => f._1).map(x => (x._1, x._2.size.toLong))
        val partnerUserCountRDD = sc.parallelize(partner_user.map(x => (x._1, x._2))).map(f => (f._1, f._2.map { x => x.toString()})).map(f => for(i <- f._2) yield (f._1, i)).flatMap(f => f).groupBy(f => f._2).map(x => (x._1, x._2.map(x => x._2).toList.distinct.size.toLong))
        val partnerTotalContentRDD = sc.parallelize(partner_total_content_count).map(f => (f._1, f._2.map { x => x.toString()})).map(f => for(i <- f._2) yield (f._1, i)).flatMap(f => f).groupBy(f => f._2).map(x => (x._1, x._2.size.toLong))
        val partnerLiveContentRDD = sc.parallelize(partner_live_content_count).map(f => (f._1, f._2.map { x => x.toString()})).map(f => for(i <- f._2) yield (f._1, i)).flatMap(f => f).groupBy(f => f._2).map(x => (x._1, x._2.size.toLong))
        val partnerReviewContentRDD = sc.parallelize(partner_review_content_count).map(f => (f._1, f._2.map { x => x.toString()})).map(f => for(i <- f._2) yield (f._1, i)).flatMap(f => f).groupBy(f => f._2).map(x => (x._1, x._2.size.toLong))

        val rdd3 = partnerUserCountRDD.leftOuterJoin(partnerActiveUserCountRDD).leftOuterJoin(partnerTotalContentRDD).leftOuterJoin(partnerLiveContentRDD).leftOuterJoin(partnerReviewContentRDD).map{f =>
            ContentSnapshotAlgoOutput(f._2._1._1._2.getOrElse(0), f._2._1._2.getOrElse(0), f._2._2.getOrElse(0), f._2._1._1._1._1, f._2._1._1._1._2.getOrElse(0), "all", f._1)
        }
        
        // For partner_id and author_id combinations
        val partner_author_total_content_count = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_SNAPSHOT_PARTNER_AUTHOR_CONTENT_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map{x => (x.get("usr.IL_UNIQUE_ID").asString(), x.get("cnt.createdFor").asList().toList, x.get("cnt.IL_UNIQUE_ID").asString())}
        val partner_author_live_content_count = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_SNAPSHOT_PARTNER_AUTHOR_LIVE_CONTENT_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map{x => (x.get("usr.IL_UNIQUE_ID").asString(), x.get("cnt.createdFor").asList().toList, x.get("cnt.IL_UNIQUE_ID").asString())}
        val partner_author_review_content_count = GraphQueryDispatcher.dispatch(graphDBConfig, CypherQueries.CONTENT_SNAPSHOT_PARTNER_AUTHOR_REVIEW_CONTENT_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map{x => (x.get("usr.IL_UNIQUE_ID").asString(), x.get("cnt.createdFor").asList().toList, x.get("cnt.IL_UNIQUE_ID").asString())}
        
        val partnerAuthorContentRDD = sc.parallelize(partner_author_total_content_count).map(f => (f._1, f._2.map { x => x.toString()}, f._3)).map(f => for(i <- f._2) yield ((f._1, i), f._3)).flatMap(f => f).groupBy(f => f._1).map(x => (x._1, x._2.map(x => x._2).toList.distinct.size.toLong))
        val partnerAuthorLiveContentRDD = sc.parallelize(partner_author_live_content_count).map(f => (f._1, f._2.map { x => x.toString()}, f._3)).map(f => for(i <- f._2) yield ((f._1, i), f._3)).flatMap(f => f).groupBy(f => f._1).map(x => (x._1, x._2.map(x => x._2).toList.distinct.size.toLong))
        val partnerAuthorReviewContentRDD = sc.parallelize(partner_author_review_content_count).map(f => (f._1, f._2.map { x => x.toString()}, f._3)).map(f => for(i <- f._2) yield ((f._1, i), f._3)).flatMap(f => f).groupBy(f => f._1).map(x => (x._1, x._2.map(x => x._2).toList.distinct.size.toLong))
        
        val rdd4 = partnerAuthorContentRDD.leftOuterJoin(partnerAuthorLiveContentRDD).leftOuterJoin(partnerAuthorReviewContentRDD).map{f =>
            ContentSnapshotAlgoOutput(f._2._1._1, f._2._1._2.getOrElse(0), f._2._2.getOrElse(0), 0, 0, f._1._1, f._1._2)
        }
        rdd1++(rdd2)++(rdd3)++(rdd4);
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