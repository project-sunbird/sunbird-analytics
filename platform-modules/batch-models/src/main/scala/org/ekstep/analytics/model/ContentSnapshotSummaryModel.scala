/**
 * @author Sowmya Dixit
 */
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
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.updater.CEUsageSummaryFact
import org.ekstep.analytics.updater.CEUsageSummaryIndex

/**
 * Case Class for the data product
 */
case class ContentSnapshotAlgoOutput(app_id: String, channel: String, total_content_count: Long, live_content_count: Long, review_content_count: Long, creation_ts: Double, avg_creation_ts: Double, total_user_count: Long = 0L, active_user_count: Long = 0L, author_id: String = "all", partner_id: String = "all") extends AlgoOutput
case class ContentSnapshotIndex(app_id: String, channel: String, author_id: String, partner_id: String)

/**
 * @dataproduct
 * @Summarizer
 *
 * ContentSnapshotSummaryModel
 *
 * Functionality
 * 1. Generate author-partner specific content snapshot summary events. This would be used to compute weekly, monthly metrics.
 * Input - Vidyavaani Graph database and ce_usage_summary_fact cassandra table data.
 *
 * Dependency - UpdateContentEditorMetricsDB
 */
object ContentSnapshotSummaryModel extends IBatchModelTemplate[DerivedEvent, DerivedEvent, ContentSnapshotAlgoOutput, MeasuredEvent] with Serializable {

    override def name(): String = "ContentSnapshotSummaryModel";
    implicit val className = "org.ekstep.analytics.model.ContentSnapshotSummaryModel";

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        data;
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentSnapshotAlgoOutput] = {

        val active_user_days_limit = config.getOrElse("active_user_days_limit", 30).asInstanceOf[Int];
        val days_limit_timestamp = new DateTime().minusDays(active_user_days_limit).getMillis

        val defaultAppId = AppConf.getConfig("default.creation.app.id");
        val defaultChannel = AppConf.getConfig("default.channel.id");
        
        // Fetch content time spent from CE Usage Summary Fact table
        val contentTimespent = sc.cassandraTable[CEUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY).where("d_period=?", 0).map(x => (x.d_app_id, x.d_channel, x.d_content_id, x.total_ts)).filter(f => ((!"all".equals(f._3)) && f._4 > 0)).collect()
        val contentTimespentB = sc.broadcast(contentTimespent)
        // For author_id = partner_id = all
        val totalUserCount = GraphQueryDispatcher.dispatch(CypherQueries.CONTENT_SNAPSHOT_TOTAL_USER_COUNT).list()
            .toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
            .map { x => (ContentSnapshotIndex(x.get("appId", defaultAppId), x.get("channel", defaultChannel), "all", "all"), x.get("userCount").asLong()) }
        val activeUsers = GraphQueryDispatcher.dispatch(CypherQueries.CONTENT_SNAPSHOT_ACTIVE_USER_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map { x =>
            val ts = CommonUtil.getTimestamp(x.get("cnt.createdOn").asString(), CommonUtil.df5, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            (ContentSnapshotIndex(x.get("appId", defaultAppId), x.get("channel", defaultChannel), "all", "all"), (x.get("usr.IL_UNIQUE_ID").asString(), ts))
        }
        val activeUserCount = sc.parallelize(activeUsers).groupByKey.map { f =>
            (f._1, f._2.filter(x => x._2 >= days_limit_timestamp).toList.map(x => x._1).distinct.size.toLong)
        }

        val userCounts = sc.parallelize(totalUserCount).fullOuterJoin(activeUserCount).map(f => (f._1, (f._2._1.getOrElse(0L), f._2._2.getOrElse(0L))))
        val contentCountByStatus = GraphQueryDispatcher.dispatch(CypherQueries.CONTENT_COUNT_BY_STATUS).list()
            .toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
            .map { x => (ContentSnapshotIndex(x.get("appId", defaultAppId), x.get("channel", defaultChannel), "all", "all"), (x.get("status").asString(), x.get("count").asLong())) } //.toMap;
        val contentCounts = sc.parallelize(contentCountByStatus).groupByKey.map { f =>
            val contentCountMap = f._2.toMap
            val totalCount = contentCountMap.values.reduce((a, b) => a + b);
            val liveCount = contentCountMap.getOrElse("live", 0).asInstanceOf[Number].longValue();
            val reviewCount = contentCountMap.getOrElse("review", 0).asInstanceOf[Number].longValue();
            val ts = contentTimespentB.value.filter(x => f._1.app_id.equals(x._1) && f._1.channel.equals(x._2))
            val creationTs = CommonUtil.roundDouble(contentTimespent.map(f => f._4).sum, 2)
            val avgCreationTs = if (ts.size > 0) CommonUtil.roundDouble((creationTs / ts.size), 2) else 0.0
            (f._1, (totalCount, liveCount, reviewCount, creationTs, avgCreationTs))
        }

        val rdd1 = userCounts.fullOuterJoin(contentCounts).map { f =>
            val userCounts = f._2._1.getOrElse(0L, 0L)
            val otherDetails = f._2._2.getOrElse(0L, 0L, 0L, 0.0, 0.0)
            ContentSnapshotAlgoOutput(f._1.app_id, f._1.channel, otherDetails._1, otherDetails._2, otherDetails._3, otherDetails._4, otherDetails._5, userCounts._1, userCounts._2, f._1.author_id, f._1.partner_id)
        }

        // For specific author_id
        val contentCountPerAuthorByStatus = GraphQueryDispatcher.dispatch(CypherQueries.CONTENT_COUNT_PER_AUTHOR_BY_STATUS).list()
            .toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
            .map { x => (ContentSnapshotIndex(x.get("appId", defaultAppId), x.get("channel", defaultChannel), x.get("identifier").asString(), "all"), (x.get("status").asString(), x.get("count").asLong())) } //.groupBy(f => f._1);

        val authorContentTSRDD = sc.makeRDD(GraphQueryDispatcher.dispatch(CypherQueries.AUTHOR_CONTENT_LIST).list()
            .toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
            .map { x => (ContentSnapshotIndex(x.get("appId", defaultAppId), x.get("channel", defaultChannel), x.get("author").asString(), "all"), x.get("contentList").asList().toList.map { x => x.toString() }) }
            .map { x =>
                val ts = contentTimespentB.value.filter(f => x._1.app_id.equals(f._1) && x._1.channel.equals(f._2))
                val contentTsList = ts.filter(f => x._2.contains(f._3)).filter(f => f._4 > 0.0)
                val creationTs = CommonUtil.roundDouble(contentTsList.map(f => f._4).sum, 2)
                val avgCreationTs = if (contentTsList.size > 0) CommonUtil.roundDouble((creationTs / contentTsList.size), 2) else 0.0
                (x._1, (creationTs, avgCreationTs))
            })
        val authorContentCounts = sc.parallelize(contentCountPerAuthorByStatus).groupByKey.map { f =>
            val contentCountMap = f._2.toMap
            val totalCount = contentCountMap.values.reduce((a, b) => a + b);
            val liveCount = contentCountMap.getOrElse("live", 0).asInstanceOf[Number].longValue();
            val reviewCount = contentCountMap.getOrElse("review", 0).asInstanceOf[Number].longValue();
            (f._1, (totalCount, liveCount, reviewCount))
        }

        val rdd2 = authorContentCounts.fullOuterJoin(authorContentTSRDD).map { f =>
            val ts = f._2._2.getOrElse(0.0, 0.0)
            val counts = f._2._1.getOrElse(0L, 0L, 0L)
            ContentSnapshotAlgoOutput(f._1.app_id, f._1.channel, counts._1, counts._2, counts._3, ts._1, ts._2, 0, 0, f._1.author_id, f._1.partner_id);
        }

        // For specific partner_id
        val partner_user = GraphQueryDispatcher.dispatch(CypherQueries.CONTENT_SNAPSHOT_PARTNER_USER_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
        .map { x => (x.get("appId", defaultAppId), x.get("channel", defaultChannel), x.get("usr.IL_UNIQUE_ID").asString(), x.get("cnt.createdFor").asList().toList, x.get("cnt.createdOn").asString()) }
        val partner_active_users = partner_user.map { x =>
            val ts = CommonUtil.getTimestamp(x._5, CommonUtil.df5, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            val partners = x._4.map { x => x.toString() }.filterNot { x => StringUtils.isBlank(x) }
            (for (i <- partners) yield (ContentSnapshotIndex(x._1, x._2, "all", i), x._3, ts))
        }.flatMap(f => f)
        val partnerActiveUserCountRDD = sc.parallelize(partner_active_users).filter(f => f._3 >= days_limit_timestamp).groupBy(f => f._1).map(x => (x._1, x._2.size.toLong))
        val partnerUserCountRDD = sc.parallelize(partner_user.map(f => (f._1, f._2, f._3, f._4.map { x => x.toString() }.filterNot { x => StringUtils.isBlank(x) })).map(f => for (i <- f._4) yield (ContentSnapshotIndex(f._1, f._2, "all", i), f._3)).flatMap(f => f).groupBy(f => f._1).map(x => (x._1, x._2.map(x => x._2).toList.distinct.size.toLong)).toSeq)

        val partnerUserCounts = partnerUserCountRDD.fullOuterJoin(partnerActiveUserCountRDD).map(f => (f._1, (f._2._1.getOrElse(0L), f._2._2.getOrElse(0L))))
        
        val partnerContentTSRDD = sc.makeRDD(GraphQueryDispatcher.dispatch(CypherQueries.PARTNER_CONTENT_LIST).list()
            .toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
            .map { x => (ContentSnapshotIndex(x.get("appId", defaultAppId), x.get("channel", defaultChannel), "all", x.get("partner").asString()), x.get("contentList").asList().toList.map { x => x.toString() }) }.filterNot { x => StringUtils.isBlank(x._1.partner_id) }
            .map { x =>
                val ts = contentTimespentB.value.filter(f => x._1.app_id.equals(f._1) && x._1.channel.equals(f._2))
                val contentTsList = ts.filter(f => x._2.contains(f._3)).filter(f => f._4 > 0.0)
                val creationTs = CommonUtil.roundDouble(contentTsList.map(f => f._4).sum, 2)
                val avgCreationTs = if (contentTsList.size > 0) CommonUtil.roundDouble((creationTs / contentTsList.size), 2) else 0.0
                (x._1, (creationTs, avgCreationTs))
            })

        val contentCountPerPartnerByStatus = GraphQueryDispatcher.dispatch(CypherQueries.CONTENT_COUNT_PER_PARTNER_BY_STATUS).list()
            .toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
            .map { x => (x.get("appId", defaultAppId), x.get("channel", defaultChannel), x.get("identifier").asList().toList.map { x => x.toString() }.filterNot { x => StringUtils.isBlank(x) }, x.get("status").asString(), x.get("count").asLong()) }
            .map(f => for (i <- f._3) yield (ContentSnapshotIndex(f._1, f._2, "all", i), (f._4, f._5))).flatMap(f => f).groupBy(f => f._1).map(x => (x._1, x._2.map(f => f._2))).toSeq

        val partnerContentCounts = sc.parallelize(contentCountPerPartnerByStatus).map { f =>
            val contentCountMap = f._2.groupBy(f => f._1).map(x => (x._1, x._2.map(f => f._2).sum))
            val totalCount = contentCountMap.values.reduce((a, b) => a + b);
            val liveCount = contentCountMap.getOrElse("live", 0).asInstanceOf[Number].longValue();
            val reviewCount = contentCountMap.getOrElse("review", 0).asInstanceOf[Number].longValue();
            (f._1, (totalCount, liveCount, reviewCount))
        }    
            
        val rdd3 = partnerUserCounts.fullOuterJoin(partnerContentTSRDD).fullOuterJoin(partnerContentCounts).map { f =>
            val contentCounts = f._2._2.getOrElse(0L, 0L, 0L)
            val ts = f._2._1.getOrElse(Option(0L, 0L), Option(0.0, 0.0))._2.getOrElse(0.0, 0.0)
            val userCounts = f._2._1.getOrElse(Option(0L, 0L), Option(0.0, 0.0))._1.getOrElse(0L, 0L)
            ContentSnapshotAlgoOutput(f._1.app_id, f._1.channel, contentCounts._1, contentCounts._2, contentCounts._3, ts._1, ts._2, userCounts._1, userCounts._2, f._1.author_id, f._1.partner_id);
        }

        // For partner_id and author_id combinations
        val contentCountPerPartnerPerAuthorByStatus = GraphQueryDispatcher.dispatch(CypherQueries.CONTENT_COUNT_PER_AUTHOR_PER_PARTNER_BY_STATUS).list()
            .toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
            .map { x => (x.get("appId", defaultAppId), x.get("channel", defaultChannel), x.get("author").asString(), x.get("partner").asList().toList.map { x => x.toString() }.filterNot { x => StringUtils.isBlank(x) }, x.get("status").asString(), x.get("count").asLong()) }
            .map(f => for (i <- f._4) yield (ContentSnapshotIndex(f._1, f._2, f._3, i), (f._5, f._6))).flatMap(f => f).groupBy(f => f._1).map(x => (x._1, x._2.map(f => f._2))).toSeq

        val authorPartnerContentTSRDD = sc.makeRDD(GraphQueryDispatcher.dispatch(CypherQueries.AUTHOR_PARTNER_CONTENT_LIST).list()
            .toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
            .map { x => (ContentSnapshotIndex(x.get("appId", defaultAppId), x.get("channel", defaultChannel), x.get("author").asString(), x.get("partner").asString()), x.get("contentList").asList().toList.map { x => x.toString() }) }.filterNot { x => StringUtils.isBlank(x._1.partner_id) }
            .map { x =>
                val ts = contentTimespentB.value.filter(f => x._1.app_id.equals(f._1) && x._1.channel.equals(f._2))
                val contentTsList = ts.filter(f => x._2.contains(f._3)).filter(f => f._4 > 0.0)
                val creationTs = CommonUtil.roundDouble(contentTsList.map(f => f._4).sum, 2)
                val avgCreationTs = if (contentTsList.size > 0) CommonUtil.roundDouble((creationTs / contentTsList.size), 2) else 0.0
                (x._1, (creationTs, avgCreationTs))
            })

        val authorPartnerContentCountRDD = sc.makeRDD(contentCountPerPartnerPerAuthorByStatus.map { f =>
            val contentCountMap = f._2.groupBy(f => f._1).map(x => (x._1, x._2.map(f => f._2).sum))
            val totalCount = contentCountMap.values.reduce((a, b) => a + b);
            val liveCount = contentCountMap.getOrElse("live", 0).asInstanceOf[Number].longValue();
            val reviewCount = contentCountMap.getOrElse("review", 0).asInstanceOf[Number].longValue();
            (f._1, (totalCount, liveCount, reviewCount))
        }.toList)

        val rdd4 = authorPartnerContentCountRDD.fullOuterJoin(authorPartnerContentTSRDD).map { f =>
            val contentCounts = f._2._1.getOrElse(0L, 0L, 0L)
            val ts = f._2._2.getOrElse(0.0, 0.0)
            ContentSnapshotAlgoOutput(f._1.app_id, f._1.channel, contentCounts._1, contentCounts._2, contentCounts._3, ts._1, ts._2, 0, 0, f._1.author_id, f._1.partner_id);
        }

        rdd1 ++ (rdd2) ++ (rdd3) ++ (rdd4);
    }

    override def postProcess(data: RDD[ContentSnapshotAlgoOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {
        val meEventVersion = AppConf.getConfig("telemetry.version");
        data.map { x =>
            val mid = CommonUtil.getMessageId("ME_CONTENT_SNAPSHOT_SUMMARY", x.author_id + x.partner_id, "SNAPSHOT", DtRange(System.currentTimeMillis(), System.currentTimeMillis()), "NA", Option(x.app_id), Option(x.channel));
            val measures = Map(
                "total_user_count" -> x.total_user_count,
                "active_user_count" -> x.active_user_count,
                "total_content_count" -> x.total_content_count,
                "live_content_count" -> x.live_content_count,
                "review_content_count" -> x.review_content_count,
                "creation_ts" -> x.creation_ts,
                "avg_creation_ts" -> x.avg_creation_ts);
            MeasuredEvent("ME_CONTENT_SNAPSHOT_SUMMARY", System.currentTimeMillis(), System.currentTimeMillis(), meEventVersion, mid, "", x.channel, None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String], Option(config.getOrElse("modelId", "ContentSnapshotSummarizer").asInstanceOf[String])), None, config.getOrElse("granularity", "SNAPSHOT").asInstanceOf[String], DtRange(System.currentTimeMillis(), System.currentTimeMillis())),
                Dimensions(None, None, None, None, None, None, Option(PData(x.app_id, "1.0")), None, None, None, None, None, None, None, None, None, None, None, None, None, None, Option(x.author_id), Option(x.partner_id)),
                MEEdata(measures));
        }
    }

}