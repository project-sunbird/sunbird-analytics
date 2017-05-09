package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.DerivedEvent
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.util.Constants
import com.pygmalios.reactiveinflux._
import java.net.URI
import org.ekstep.analytics.framework.conf.AppConf
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class TestUpdateContentSnapshotDB extends SparkSpec(null) {

    it should "update the content snapshot updater db and check the updated fields" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.content_snapshot_summary");
        }
        implicit val awaitAtMost = 10.seconds

        val rdd = loadFile[DerivedEvent]("src/test/resources/content-snapshot-updater/test_data1.json");
        val rdd1 = UpdateContentSnapshotDB.execute(rdd, None);
        
        val snapshotData1 = sc.cassandraTable[ContentSnapshotSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SNAPSHOT_SUMMARY).collect

        // Check for DAY record
        val record1 = snapshotData1.filter { x => ("all".equals(x.d_author_id)) && ("all".equals(x.d_partner_id)) && (20170425 == x.d_period) }.last
        record1.total_author_count should be(2)
        record1.total_author_count_start should be(record1.total_author_count)
        record1.active_author_count should be(0)
        record1.active_author_count_start should be(record1.active_author_count)
        record1.total_content_count should be(4)
        record1.total_content_count_start should be(record1.total_content_count)
        record1.live_content_count should be(1)
        record1.live_content_count_start should be(record1.live_content_count)
        record1.review_content_count should be(0)
        record1.review_content_count_start should be(record1.review_content_count)
        
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("select * from content_snapshot_metrics where author_id = 'all' AND partner_id = 'all' AND period = 'day'")
            queryResult.result.isEmpty should be(false)
            val res = getValuesMap(queryResult, 0)
            res.get("author_id").get.contains("all") should be(true)
            res.get("partner_id").get.contains("all") should be(true)
            res.get("period").get.contains("day") should be(true)
            res.get("total_author_count").get.toLong should be(2)
            res.get("total_author_count_start").get.toLong should be(2)
            res.get("active_author_count").get.toLong should be(0)
            res.get("active_author_count_start").get.toLong should be(0)
            res.get("total_content_count").get.toLong should be(4)
            res.get("total_content_count_start").get.toLong should be(4)
            res.get("live_content_count").get.toLong should be(1)
            res.get("live_content_count_start").get.toLong should be(1)
            res.get("review_content_count").get.toLong should be(0)
            res.get("review_content_count_start").get.toLong should be(0)
        }
        
        // Check for WEEK record
        val record2 = snapshotData1.filter { x => ("290".equals(x.d_author_id)) && ("all".equals(x.d_partner_id)) && (2017717 == x.d_period) }.last
        record2.total_author_count should be(0)
        record2.total_author_count_start should be(record2.total_author_count)
        record2.active_author_count should be(0)
        record2.active_author_count_start should be(record2.active_author_count)
        record2.total_content_count should be(3)
        record2.total_content_count_start should be(record2.total_content_count)
        record2.live_content_count should be(1)
        record2.live_content_count_start should be(record2.live_content_count)
        record2.review_content_count should be(0)
        record2.review_content_count_start should be(record2.review_content_count)
        
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("select * from content_snapshot_metrics where author_id = '290' AND partner_id = 'all' AND period = 'week'")
            queryResult.result.isEmpty should be(false)
            val res = getValuesMap(queryResult, 0)
            res.get("author_id").get.contains("290") should be(true)
            res.get("partner_id").get.contains("all") should be(true)
            res.get("period").get.contains("week") should be(true)
            res.get("total_author_count").get.toLong should be(0)
            res.get("total_author_count_start").get.toLong should be(0)
            res.get("active_author_count").get.toLong should be(0)
            res.get("active_author_count_start").get.toLong should be(0)
            res.get("total_content_count").get.toLong should be(3)
            res.get("total_content_count_start").get.toLong should be(3)
            res.get("live_content_count").get.toLong should be(1)
            res.get("live_content_count_start").get.toLong should be(1)
            res.get("review_content_count").get.toLong should be(0)
            res.get("review_content_count_start").get.toLong should be(0)
        }
        
        val rdd2 = loadFile[DerivedEvent]("src/test/resources/content-snapshot-updater/test_data2.json");
        val rdd3 = UpdateContentSnapshotDB.execute(rdd2, None);
        
        val snapshotData2 = sc.cassandraTable[ContentSnapshotSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SNAPSHOT_SUMMARY).collect
        
        // Check for same DAY record
        val record3 = snapshotData2.filter { x => ("all".equals(x.d_author_id)) && ("all".equals(x.d_partner_id)) && (20170425 == x.d_period) }.last
        record3.total_author_count should be(2)
        record3.total_author_count_start should be(record3.total_author_count)
        record3.active_author_count should be(0)
        record3.active_author_count_start should be(record3.active_author_count)
        record3.total_content_count should be(4)
        record3.total_content_count_start should be(record3.total_content_count)
        record3.live_content_count should be(1)
        record3.live_content_count_start should be(record3.live_content_count)
        record3.review_content_count should be(0)
        record3.review_content_count_start should be(record3.review_content_count)
        
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("select * from content_snapshot_metrics where author_id = 'all' AND partner_id = 'all' AND period = 'day'")
            queryResult.result.isEmpty should be(false)
            val res = getValuesMap(queryResult, 0)
            res.get("author_id").get.contains("all") should be(true)
            res.get("partner_id").get.contains("all") should be(true)
            res.get("period").get.contains("day") should be(true)
            res.get("total_author_count").get.toLong should be(2)
            res.get("total_author_count_start").get.toLong should be(2)
            res.get("active_author_count").get.toLong should be(0)
            res.get("active_author_count_start").get.toLong should be(0)
            res.get("total_content_count").get.toLong should be(4)
            res.get("total_content_count_start").get.toLong should be(4)
            res.get("live_content_count").get.toLong should be(1)
            res.get("live_content_count_start").get.toLong should be(1)
            res.get("review_content_count").get.toLong should be(0)
            res.get("review_content_count_start").get.toLong should be(0)
        }
        
        // Check for next DAY record
        val record4 = snapshotData2.filter { x => ("all".equals(x.d_author_id)) && ("all".equals(x.d_partner_id)) && (20170426 == x.d_period) }.last
        record4.total_author_count should be(10)
        record4.total_author_count_start should be(record4.total_author_count)
        record4.active_author_count should be(2)
        record4.active_author_count_start should be(record4.active_author_count)
        record4.total_content_count should be(6)
        record4.total_content_count_start should be(record4.total_content_count)
        record4.live_content_count should be(3)
        record4.live_content_count_start should be(record4.live_content_count)
        record4.review_content_count should be(1)
        record4.review_content_count_start should be(record4.review_content_count)
        
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("select * from content_snapshot_metrics where author_id = 'all' AND partner_id = 'all' AND period = 'day'")
            queryResult.result.isEmpty should be(false)
            val res = getValuesMap(queryResult, 1)
            res.get("author_id").get.contains("all") should be(true)
            res.get("partner_id").get.contains("all") should be(true)
            res.get("period").get.contains("day") should be(true)
            res.get("total_author_count").get.toLong should be(10)
            res.get("total_author_count_start").get.toLong should be(10)
            res.get("active_author_count").get.toLong should be(2)
            res.get("active_author_count_start").get.toLong should be(2)
            res.get("total_content_count").get.toLong should be(6)
            res.get("total_content_count_start").get.toLong should be(6)
            res.get("live_content_count").get.toLong should be(3)
            res.get("live_content_count_start").get.toLong should be(3)
            res.get("review_content_count").get.toLong should be(1)
            res.get("review_content_count_start").get.toLong should be(1)
        }
        
        // Check for same WEEK record
        val record5 = snapshotData2.filter { x => ("290".equals(x.d_author_id)) && ("all".equals(x.d_partner_id)) && (2017717 == x.d_period) }.last
        record5.total_author_count should be(0)
        record5.total_author_count_start should be(0)
        record5.active_author_count should be(0)
        record5.active_author_count_start should be(0)
        record5.total_content_count should be(6)
        record5.total_content_count_start should be(3)
        record5.live_content_count should be(3)
        record5.live_content_count_start should be(1)
        record5.review_content_count should be(1)
        record5.review_content_count_start should be(0)
        
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("select * from content_snapshot_metrics where author_id = '290' AND partner_id = 'all' AND period = 'week'")
            queryResult.result.isEmpty should be(false)
            val res = getValuesMap(queryResult, 0)
            res.get("author_id").get.contains("290") should be(true)
            res.get("partner_id").get.contains("all") should be(true)
            res.get("period").get.contains("week") should be(true)
            res.get("total_author_count").get.toLong should be(0)
            res.get("total_author_count_start").get.toLong should be(0)
            res.get("active_author_count").get.toLong should be(0)
            res.get("active_author_count_start").get.toLong should be(0)
            res.get("total_content_count").get.toLong should be(6)
            res.get("total_content_count_start").get.toLong should be(3)
            res.get("live_content_count").get.toLong should be(3)
            res.get("live_content_count_start").get.toLong should be(1)
            res.get("review_content_count").get.toLong should be(1)
            res.get("review_content_count_start").get.toLong should be(0)
        }
    }
    
    def getValuesMap(queryResult: QueryResult, rowIndex: Int): Map[String, String] = {
        val cols = sc.parallelize(queryResult.result.singleSeries.columns).zipWithIndex().map { case (k, v) => (v, k) }
        val vals = sc.parallelize(queryResult.result.singleSeries.rows(rowIndex).values).zipWithIndex().map { case (k, v) => (v, k) }
        cols.leftOuterJoin(vals).map(x => x._2).map(f => (f._1.toString(), f._2.get.mkString)).collect().toMap  
    }
        
}