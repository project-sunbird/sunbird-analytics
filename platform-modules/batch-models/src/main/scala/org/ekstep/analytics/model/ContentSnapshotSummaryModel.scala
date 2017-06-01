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

/**
 * Case Class for the data product
 */
case class ContentSnapshotAlgoOutput(total_content_count: Long, live_content_count: Long, review_content_count: Long, creation_ts: Double, avg_creation_ts: Double, total_user_count: Long = 0L, active_user_count: Long = 0L, author_id: String = "all", partner_id: String = "all") extends AlgoOutput

/**
 * @dataproduct
 * @Summarizer
 *
 * ContentSnapshotSummaryModel
 *
 * Functionality
 * 1. Generate author-partner specific content snapshot summary events. This would be used to compute weekly, monthly metrics.
 * Input - Vidyavaani Graph database and ce_usage_summary_fact cassandra table data.
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

        // Fetch content time spent from CE Usage Summary Fact table
        val contentTimespent = sc.cassandraTable[CEUsageSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CE_USAGE_SUMMARY).where("d_period=?", 0).map(x => (x.d_content_id, x.total_ts)).filter(f => ((!"all".equals(f._1)) && f._2 > 0))
        val contentTimespentMap = contentTimespent.collect().toMap

        // For author_id = all
        val total_user_count = GraphQueryDispatcher.dispatch(CypherQueries.CONTENT_SNAPSHOT_TOTAL_USER_COUNT).list().get(0).get("count(usr)").asLong();
        val active_users = GraphQueryDispatcher.dispatch(CypherQueries.CONTENT_SNAPSHOT_ACTIVE_USER_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map { x =>
            val ts = CommonUtil.getTimestamp(x.get("cnt.createdOn").asString(), CommonUtil.df5, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            (x.get("usr.IL_UNIQUE_ID").asString(), ts)
        }
        val active_user_count = sc.parallelize(active_users).filter(f => f._2 >= days_limit_timestamp).map(x => x._1).distinct().count();

        val contentCountByStatus = GraphQueryDispatcher.dispatch(CypherQueries.CONTENT_COUNT_BY_STATUS).list()
            .toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
            .map { x => (x.get("status").asString(), x.get("count").asLong()) }.toMap;

        val totalContentCount = contentCountByStatus.values.reduce((a, b) => a + b);
        val liveContentCount = contentCountByStatus.getOrElse("live", 0).asInstanceOf[Number].longValue();
        val reviewContentCount = contentCountByStatus.getOrElse("review", 0).asInstanceOf[Number].longValue();
        val creationTs = contentTimespent.map(f => f._2).sum
        val avgCreationTs = if (contentTimespent.count() > 0) creationTs / contentTimespent.count() else 0.0
        val rdd1 = sc.makeRDD(List(ContentSnapshotAlgoOutput(totalContentCount, liveContentCount, reviewContentCount, creationTs, avgCreationTs, total_user_count, active_user_count)))

        // For specific author_id
        val contentCountPerAuthorByStatus = GraphQueryDispatcher.dispatch(CypherQueries.CONTENT_COUNT_PER_AUTHOR_BY_STATUS).list()
            .toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
            .map { x => (x.get("identifier").asString(), x.get("status").asString(), x.get("count").asLong()) }.groupBy(f => f._1);

        val authorContentTSRDD = sc.makeRDD(GraphQueryDispatcher.dispatch(CypherQueries.AUTHOR_CONTENT_LIST).list()
            .toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
            .map { x => (x.get("author").asString(), x.get("contentList").asList().toList.map { x => x.toString() }) }
            .map { x =>
                val contentTsList = for (f <- x._2) yield (f, contentTimespentMap.get(f).getOrElse(0.0))
                val filteredContentTsList = contentTsList.filter(f => f._2 > 0.0)
                val ts = filteredContentTsList.map(f => f._2).sum
                val avg_ts = if (filteredContentTsList.size > 0) ts / filteredContentTsList.size else 0.0
                (x._1, (ts, avg_ts))
            })

        val authorContentCount = sc.makeRDD(contentCountPerAuthorByStatus.map { f =>
            val total = f._2.map(f => f._3).reduce((a, b) => a + b);
            val live = f._2.filter(f => StringUtils.equals("live", f._2)).headOption.getOrElse((f._1, "live", 0L))._3;
            val review = f._2.filter(f => StringUtils.equals("review", f._2)).headOption.getOrElse((f._1, "review", 0L))._3;
            (f._1, (total, live, review))
        }.toList)

        val rdd2 = authorContentCount.leftOuterJoin(authorContentTSRDD).map { f =>
            val ts = f._2._2.getOrElse(0.0, 0.0)
            ContentSnapshotAlgoOutput(f._2._1._1, f._2._1._2, f._2._1._3, ts._1, ts._2, 0, 0, f._1);
        }

        // For specific partner_id
        val partner_user = GraphQueryDispatcher.dispatch(CypherQueries.CONTENT_SNAPSHOT_PARTNER_USER_COUNT).list().toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }.map { x => (x.get("usr.IL_UNIQUE_ID").asString(), x.get("cnt.createdFor").asList().toList, x.get("cnt.createdOn").asString()) }
        val partner_active_users = partner_user.map { x =>
            val ts = CommonUtil.getTimestamp(x._3, CommonUtil.df5, "yyyy-MM-dd'T'HH:mm:ss.SSSZ");
            val partners = x._2.map { x => x.toString() }.filterNot { x => StringUtils.isBlank(x) }
            (for (i <- partners) yield (i, x._1, ts))
        }.flatMap(f => f)
        val partnerActiveUserCountRDD = sc.parallelize(partner_active_users).filter(f => f._3 >= days_limit_timestamp).groupBy(f => f._1).map(x => (x._1, x._2.size.toLong))
        val partnerUserCountRDD = sc.parallelize(partner_user.map(x => (x._1, x._2))).map(f => (f._1, f._2.map { x => x.toString() }.filterNot { x => StringUtils.isBlank(x) })).map(f => for (i <- f._2) yield (f._1, i)).flatMap(f => f).groupBy(f => f._2).map(x => (x._1, x._2.map(x => x._2).toList.distinct.size.toLong))

        val partnerContentTSRDD = sc.makeRDD(GraphQueryDispatcher.dispatch(CypherQueries.PARTNER_CONTENT_LIST).list()
            .toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
            .map { x => (x.get("partner").asString(), x.get("contentList").asList().toList.map { x => x.toString() }) }
            .map { x =>
                val contentTsList = for (f <- x._2) yield (f, contentTimespentMap.get(f).getOrElse(0.0))
                val filteredContentTsList = contentTsList.filter(f => f._2 > 0.0)
                val ts = filteredContentTsList.map(f => f._2).sum
                val avg_ts = if (filteredContentTsList.size > 0) ts / filteredContentTsList.size else 0.0
                (x._1, (ts, avg_ts))
            })

        val contentCountPerPartnerByStatus = GraphQueryDispatcher.dispatch(CypherQueries.CONTENT_COUNT_PER_PARTNER_BY_STATUS).list()
            .toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
            .map { x => (x.get("identifier").asList().toList.map { x => x.toString() }.filterNot { x => StringUtils.isBlank(x) }, x.get("status").asString(), x.get("count").asLong()) }
            .map(f => for (i <- f._1) yield (i, f._2, f._3)).flatMap(f => f).groupBy(f => f._1)

        val rdd3 = partnerUserCountRDD.leftOuterJoin(partnerActiveUserCountRDD).leftOuterJoin(partnerContentTSRDD).map { f =>
            val contentCount = contentCountPerPartnerByStatus.get(f._1).get
            val total = contentCount.map(f => f._3).reduce((a, b) => a + b);
            val live = contentCount.filter(f => StringUtils.equals("live", f._2)).headOption.getOrElse((f._1, "live", 0L))._3;
            val review = contentCount.filter(f => StringUtils.equals("review", f._2)).headOption.getOrElse((f._1, "review", 0L))._3;
            val ts = f._2._2.getOrElse(0.0, 0.0)
            ContentSnapshotAlgoOutput(total, live, review, ts._1, ts._2, f._2._1._1, f._2._1._2.getOrElse(0), "all", f._1);
        }

        // For partner_id and author_id combinations
        val contentCountPerPartnerPerAuthorByStatus = GraphQueryDispatcher.dispatch(CypherQueries.CONTENT_COUNT_PER_AUTHOR_PER_PARTNER_BY_STATUS).list()
            .toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
            .map { x => (x.get("author").asString(), x.get("partner").asList().toList.map { x => x.toString() }.filterNot { x => StringUtils.isBlank(x) }, x.get("status").asString(), x.get("count").asLong()) }
            .map(f => for (i <- f._2) yield ((f._1, i), f._3, f._4)).flatMap(f => f).groupBy(f => f._1)

        val authorPartnerContentTSRDD = sc.makeRDD(GraphQueryDispatcher.dispatch(CypherQueries.AUTHOR_PARTNER_CONTENT_LIST).list()
            .toArray().map { x => x.asInstanceOf[org.neo4j.driver.v1.Record] }
            .map { x => (x.get("author").asString(), x.get("partner").asString(), x.get("contentList").asList().toList.map { x => x.toString() }) }
            .map { x =>
                val contentTsList = for (f <- x._3) yield (f, contentTimespentMap.get(f).getOrElse(0.0))
                val filteredContentTsList = contentTsList.filter(f => f._2 > 0.0)
                val ts = filteredContentTsList.map(f => f._2).sum
                val avg_ts = if (filteredContentTsList.size > 0) ts / filteredContentTsList.size else 0.0
                ((x._1, x._2), (ts, avg_ts))
            })

        val authorPartnerContentCountRDD = sc.makeRDD(contentCountPerPartnerPerAuthorByStatus.map { f =>
            val total = f._2.map(f => f._3).reduce((a, b) => a + b);
            val live = f._2.filter(f => StringUtils.equals("live", f._2)).headOption.getOrElse((f._1, "live", 0L))._3;
            val review = f._2.filter(f => StringUtils.equals("review", f._2)).headOption.getOrElse((f._1, "review", 0L))._3;
            ((f._1._1, f._1._2), (total, live, review));
        }.toList)

        val rdd4 = authorPartnerContentCountRDD.leftOuterJoin(authorPartnerContentTSRDD).map { f =>
            val ts = f._2._2.getOrElse(0.0, 0.0)
            ContentSnapshotAlgoOutput(f._2._1._1, f._2._1._2, f._2._1._3, ts._1, ts._2, 0, 0, f._1._1, f._1._2);
        }

        rdd1 ++ (rdd2) ++ (rdd3) ++ (rdd4);
    }

    override def postProcess(data: RDD[ContentSnapshotAlgoOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[MeasuredEvent] = {

        data.map { x =>
            val mid = CommonUtil.getMessageId("ME_CONTENT_SNAPSHOT_SUMMARY", x.author_id, "SNAPSHOT", DtRange(System.currentTimeMillis(), System.currentTimeMillis()));
            val measures = Map(
                "total_user_count" -> x.total_user_count,
                "active_user_count" -> x.active_user_count,
                "total_content_count" -> x.total_content_count,
                "live_content_count" -> x.live_content_count,
                "review_content_count" -> x.review_content_count,
                "creation_ts" -> x.creation_ts,
                "avg_creation_ts" -> x.avg_creation_ts);
            MeasuredEvent("ME_CONTENT_SNAPSHOT_SUMMARY", System.currentTimeMillis(), System.currentTimeMillis(), "1.0", mid, "", None, None,
                Context(PData(config.getOrElse("producerId", "AnalyticsDataPipeline").asInstanceOf[String], config.getOrElse("modelId", "ContentSnapshotSummarizer").asInstanceOf[String], config.getOrElse("modelVersion", "1.0").asInstanceOf[String]), None, config.getOrElse("granularity", "SNAPSHOT").asInstanceOf[String], DtRange(System.currentTimeMillis(), System.currentTimeMillis())),
                Dimensions(None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, Option(x.author_id), Option(x.partner_id)),
                MEEdata(measures));
        }
    }

}