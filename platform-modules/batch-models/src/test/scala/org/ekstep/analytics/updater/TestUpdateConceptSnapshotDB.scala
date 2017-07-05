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
import org.apache.commons.lang3.StringUtils

class TestUpdateConceptSnapshotDB extends SparkSpec(null) {
  
    it should "update the concept snapshot updater db and check the updated fields" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.concept_snapshot_summary");
        }
        implicit val awaitAtMost = 10.seconds

        val rdd = loadFile[DerivedEvent]("src/test/resources/concept-snapshot-updater/test_data1.json");
        val rdd1 = UpdateConceptSnapshotDB.execute(rdd, None);
        
        val snapshotData1 = sc.cassandraTable[ConceptSnapshotSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONCEPT_SNAPSHOT_SUMMARY).collect

        // Check for DAY record
        val record1 = snapshotData1.filter { x => ("Num:C1:SC1".equals(x.d_concept_id)) && (20170426 == x.d_period) }.last
        record1.total_content_count should be(1)
        record1.total_content_count_start should be(record1.total_content_count)
        record1.live_content_count should be(1)
        record1.live_content_count_start should be(record1.live_content_count)
        record1.review_content_count should be(0)
        record1.review_content_count_start should be(record1.review_content_count)
        
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("select * from concept_snapshot_metrics where concept_id = 'Num:C1:SC1' AND period = 'day'")
            queryResult.result.isEmpty should be(false)
            val res = getValuesMap(queryResult, "Num:C1:SC1", "day", "2017-04-26")
            res.get("concept_id").get.contains("Num:C1:SC1") should be(true)
            res.get("period").get.contains("day") should be(true)
            res.get("total_content_count").get.toLong should be(1)
            res.get("total_content_count_start").get.toLong should be(1)
            res.get("live_content_count").get.toLong should be(1)
            res.get("live_content_count_start").get.toLong should be(1)
            res.get("review_content_count").get.toLong should be(0)
            res.get("review_content_count_start").get.toLong should be(0)
        }
        
        // Check for WEEK record
        val record2 = snapshotData1.filter { x => ("Num:C1:SC1".equals(x.d_concept_id)) && (2017717 == x.d_period) }.last
        record2.total_content_count should be(1)
        record2.total_content_count_start should be(record1.total_content_count)
        record2.live_content_count should be(1)
        record2.live_content_count_start should be(record1.live_content_count)
        record2.review_content_count should be(0)
        record2.review_content_count_start should be(record1.review_content_count)
        
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("select * from concept_snapshot_metrics where concept_id = 'Num:C1:SC1' AND period = 'week'")
            queryResult.result.isEmpty should be(false)
            val res = getValuesMap(queryResult, "Num:C1:SC1", "week", "2017-04-30")
            res.get("concept_id").get.contains("Num:C1:SC1") should be(true)
            res.get("period").get.contains("week") should be(true)
            res.get("total_content_count").get.toLong should be(1)
            res.get("total_content_count_start").get.toLong should be(1)
            res.get("live_content_count").get.toLong should be(1)
            res.get("live_content_count_start").get.toLong should be(1)
            res.get("review_content_count").get.toLong should be(0)
            res.get("review_content_count_start").get.toLong should be(0)
        }
        
         // Check for MONTH record
        val record3 = snapshotData1.filter { x => ("Num:C1:SC1".equals(x.d_concept_id)) && (201704 == x.d_period) }.last
        record3.total_content_count should be(1)
        record3.total_content_count_start should be(record1.total_content_count)
        record3.live_content_count should be(1)
        record3.live_content_count_start should be(record1.live_content_count)
        record3.review_content_count should be(0)
        record3.review_content_count_start should be(record1.review_content_count)
        
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("select * from concept_snapshot_metrics where concept_id = 'Num:C1:SC1' AND period = 'month'")
            queryResult.result.isEmpty should be(false)
            val res = getValuesMap(queryResult, "Num:C1:SC1", "month", "2017-04-30")
            res.get("concept_id").get.contains("Num:C1:SC1") should be(true)
            res.get("period").get.contains("month") should be(true)
            res.get("total_content_count").get.toLong should be(1)
            res.get("total_content_count_start").get.toLong should be(1)
            res.get("live_content_count").get.toLong should be(1)
            res.get("live_content_count_start").get.toLong should be(1)
            res.get("review_content_count").get.toLong should be(0)
            res.get("review_content_count_start").get.toLong should be(0)
        }
        
        val rdd2 = loadFile[DerivedEvent]("src/test/resources/concept-snapshot-updater/test_data2.json");
        val rdd3 = UpdateConceptSnapshotDB.execute(rdd2, None);
        
        val snapshotData2 = sc.cassandraTable[ConceptSnapshotSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONCEPT_SNAPSHOT_SUMMARY).collect
        
        // Check for new DAY record
        val record4 = snapshotData2.filter { x => ("Num:C1:SC1".equals(x.d_concept_id)) && (20170427 == x.d_period) }.last
        record4.total_content_count should be(10)
        record4.total_content_count_start should be(record4.total_content_count)
        record4.live_content_count should be(4)
        record4.live_content_count_start should be(record4.live_content_count)
        record4.review_content_count should be(2)
        record4.review_content_count_start should be(record4.review_content_count)
        
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("select * from concept_snapshot_metrics where concept_id = 'Num:C1:SC1' AND period = 'day'")
            queryResult.result.isEmpty should be(false)
            val res = getValuesMap(queryResult, "Num:C1:SC1", "day", "2017-04-27")
            res.get("concept_id").get.contains("Num:C1:SC1") should be(true)
            res.get("period").get.contains("day") should be(true)
            res.get("total_content_count").get.toLong should be(10)
            res.get("total_content_count_start").get.toLong should be(10)
            res.get("live_content_count").get.toLong should be(4)
            res.get("live_content_count_start").get.toLong should be(4)
            res.get("review_content_count").get.toLong should be(2)
            res.get("review_content_count_start").get.toLong should be(2)
        }
        
        // Check for same WEEK record
        val record5 = snapshotData2.filter { x => ("Num:C1:SC1".equals(x.d_concept_id)) && (2017717 == x.d_period) }.last
        record5.total_content_count should be(10)
        record5.total_content_count_start should be(1)
        record5.live_content_count should be(4)
        record5.live_content_count_start should be(1)
        record5.review_content_count should be(2)
        record5.review_content_count_start should be(0)
        
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("select * from concept_snapshot_metrics where concept_id = 'Num:C1:SC1' AND period = 'week'")
            queryResult.result.isEmpty should be(false)
            val res = getValuesMap(queryResult, "Num:C1:SC1", "week", "2017-04-30")
            res.get("concept_id").get.contains("Num:C1:SC1") should be(true)
            res.get("period").get.contains("week") should be(true)
            res.get("total_content_count").get.toLong should be(10)
            res.get("total_content_count_start").get.toLong should be(1)
            res.get("live_content_count").get.toLong should be(4)
            res.get("live_content_count_start").get.toLong should be(1)
            res.get("review_content_count").get.toLong should be(2)
            res.get("review_content_count_start").get.toLong should be(0)
        }
        
         // Check for same MONTH record
        val record6 = snapshotData2.filter { x => ("Num:C1:SC1".equals(x.d_concept_id)) && (201704 == x.d_period) }.last
        record6.total_content_count should be(10)
        record6.total_content_count_start should be(1)
        record6.live_content_count should be(4)
        record6.live_content_count_start should be(1)
        record6.review_content_count should be(2)
        record6.review_content_count_start should be(0)
        
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("select * from concept_snapshot_metrics where concept_id = 'Num:C1:SC1' AND period = 'month'")
            queryResult.result.isEmpty should be(false)
            val res = getValuesMap(queryResult, "Num:C1:SC1", "month", "2017-04-30")
            res.get("concept_id").get.contains("Num:C1:SC1") should be(true)
            res.get("period").get.contains("month") should be(true)
            res.get("total_content_count").get.toLong should be(10)
            res.get("total_content_count_start").get.toLong should be(1)
            res.get("live_content_count").get.toLong should be(4)
            res.get("live_content_count_start").get.toLong should be(1)
            res.get("review_content_count").get.toLong should be(2)
            res.get("review_content_count_start").get.toLong should be(0)
        }
        
        // Check for new WEEK record
        val record7 = snapshotData2.filter { x => ("Num:C1:SC1".equals(x.d_concept_id)) && (2017719 == x.d_period) }.last
        record7.total_content_count should be(10)
        record7.total_content_count_start should be(record7.total_content_count)
        record7.live_content_count should be(4)
        record7.live_content_count_start should be(record7.live_content_count)
        record7.review_content_count should be(2)
        record7.review_content_count_start should be(record7.review_content_count)
        
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("select * from concept_snapshot_metrics where concept_id = 'Num:C1:SC1' AND period = 'week'")
            queryResult.result.isEmpty should be(false)
            val res = getValuesMap(queryResult, "Num:C1:SC1", "week", "2017-05-14")
            res.get("concept_id").get.contains("Num:C1:SC1") should be(true)
            res.get("period").get.contains("week") should be(true)
            res.get("total_content_count").get.toLong should be(10)
            res.get("total_content_count_start").get.toLong should be(10)
            res.get("live_content_count").get.toLong should be(4)
            res.get("live_content_count_start").get.toLong should be(4)
            res.get("review_content_count").get.toLong should be(2)
            res.get("review_content_count_start").get.toLong should be(2)
        }
        
        // Check for new MONTH record
        val record8 = snapshotData2.filter { x => ("Num:C1:SC1".equals(x.d_concept_id)) && (201705 == x.d_period) }.last
        record8.total_content_count should be(10)
        record8.total_content_count_start should be(record8.total_content_count)
        record8.live_content_count should be(4)
        record8.live_content_count_start should be(record8.live_content_count)
        record8.review_content_count should be(2)
        record8.review_content_count_start should be(record8.review_content_count)
        
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.url")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("select * from concept_snapshot_metrics where concept_id = 'Num:C1:SC1' AND period = 'month'")
            queryResult.result.isEmpty should be(false)
            val res = getValuesMap(queryResult, "Num:C1:SC1", "month", "2017-05-31")
            res.get("concept_id").get.contains("Num:C1:SC1") should be(true)
            res.get("period").get.contains("month") should be(true)
            res.get("total_content_count").get.toLong should be(10)
            res.get("total_content_count_start").get.toLong should be(10)
            res.get("live_content_count").get.toLong should be(4)
            res.get("live_content_count_start").get.toLong should be(4)
            res.get("review_content_count").get.toLong should be(2)
            res.get("review_content_count_start").get.toLong should be(2)
        }
    }
    
    def getValuesMap(queryResult: QueryResult, concept_id: String, period: String, date: String): Map[String, String] = {
        val row = queryResult.result.singleSeries.rows.toList.map{x => x.values.mkString(",")}.filter { x => x.contains(concept_id) && x.contains(period) && x.contains(date) }.head
        val values = StringUtils.split(row, ",").map{x => StringUtils.substring(x, StringUtils.indexOf(x, "(")+1, StringUtils.indexOf(x, ")"))}
        val cols = sc.parallelize(queryResult.result.singleSeries.columns).zipWithIndex().map { case (k, v) => (v, k) }
        val vals = sc.parallelize(values).zipWithIndex().map { case (k, v) => (v, k) }
        cols.leftOuterJoin(vals).map(x => x._2).map(f => (f._1.toString(), f._2.get)).collect().toMap  
    }
}