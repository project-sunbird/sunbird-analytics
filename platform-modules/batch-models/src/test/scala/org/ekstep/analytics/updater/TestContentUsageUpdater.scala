package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.MeasuredEvent
import org.joda.time.DateTime
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.job.ReplaySupervisor
import org.ekstep.analytics.framework.Dispatcher

class TestContentUsageUpdater extends SparkSpec(null) {

    //    "Content Usage Updater" should "update the content usage updater db and check the updated fields" in {
    //
    //        val sampleSumm = ContentUsageSummaryFact("org.ekstep.story.hi.vayu", 20167718, false, "Story", "application/vnd.ekstep.ecml-archive", new DateTime(1462675927499L), new DateTime(1462675927499L), 19.96d, 2, 9.98d, 7, 21.04d, None, None)
    //        val sampleRDD = sc.parallelize(Array(sampleSumm));
    //        sampleRDD.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT)
    //        val rdd = loadFile[MeasuredEvent]("src/test/resources/content-usage-updater/content_usage_updater.log");
    //        val rdd2 = ContentUsageUpdater.execute(rdd, None);
    //        val updatedSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "org.ekstep.story.hi.vayu").where("d_period=?", 20167718).first
    //        updatedSumm.m_total_ts should be(39.92)
    //        updatedSumm.m_avg_interactions_min should be(21.04d)
    //        updatedSumm.m_total_interactions should be(14)
    //        updatedSumm.m_total_sessions should be(4)
    //
    //        CassandraConnector(sc.getConf).withSessionDo { session =>
    //            session.execute("DELETE FROM content_db.content_usage_summary_fact where d_content_id = 'org.ekstep.story.hi.vayu'");
    //        }
    //    }
    //
    //    it should "update content_usage_summary_fact table for the content 'org.ekstep.delta' and pass some -ve and +ve test case" in {
    //
    //        CassandraConnector(sc.getConf).withSessionDo { session =>
    //            val query = "DELETE FROM " + Constants.CONTENT_KEY_SPACE_NAME + "." + Constants.CONTENT_USAGE_SUMMARY_FACT + " where d_content_id='org.ekstep.delta'"
    //            session.execute(query);
    //        }
    //
    //        val rdd = loadFile[MeasuredEvent]("src/test/resources/content-usage-updater/test_data.log");
    //        val rdd2 = ContentUsageUpdater.execute(rdd, None);
    //
    //        val updatedSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "org.ekstep.delta").cache
    //
    //        updatedSumm.collect().size should be(34)
    //
    //        updatedSumm.filter { x => 201602 == x.d_period }.collect().size should be(0)
    //        updatedSumm.filter { x => 201604 == x.d_period }.collect().size should be(0)
    //
    //        updatedSumm.filter { x => 201603 == x.d_period }.collect().size should be(1)
    //        updatedSumm.filter { x => 201605 == x.d_period }.collect().size should be(1)
    //
    //        val week9 = updatedSumm.filter { x => 2016779 == x.d_period }.collect()
    //        week9.size should be(1)
    //        //        val mar14 = updatedSumm.filter { x => 20160314 == x.d_period }.first()
    //        //        val mar15 = updatedSumm.filter { x => 20160315 == x.d_period }.first()
    //        //        val mar16 = updatedSumm.filter { x => 20160316 == x.d_period }.first()
    //        //        val mar17 = updatedSumm.filter { x => 20160317 == x.d_period }.first()  
    //        //        (mar14.m_total_ts + mar15.m_total_ts + mar16.m_total_ts+ mar17.m_total_ts) should be (week9.last.m_total_ts) 
    //
    //        updatedSumm.filter { x => 20167710 == x.d_period }.collect().size should be(0)
    //    }

    it should "update content_usage_summary_fact table for the content 'org.ekstep.prathamhindi1'" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            val query = "DELETE FROM " + Constants.CONTENT_KEY_SPACE_NAME + "." + Constants.CONTENT_USAGE_SUMMARY_FACT + " where d_content_id='org.ekstep.delta'"
            session.execute(query);
        }
        val rdd = loadFile[MeasuredEvent]("src/test/resources/content-usage-updater/content_usage.log").cache;
        val sortedEvents = rdd.map { x => x.syncts }.collect
        for (e <- sortedEvents) {
            val rdd2 = rdd.filter { f => (f.syncts == e) };
            ContentUsageUpdater.execute(rdd2, None);
        }

        val updatedSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "org.ekstep.delta").cache
        val grpUserFalse = updatedSumm.filter { x => (x.d_group_user == false) }.cache
        val grpUserTrue = updatedSumm.filter { x => (x.d_group_user == true) }.cache

        //timeSpent for the record with group_user = true is 0
        grpUserTrue.map{x=>x.m_total_ts}.sum should be (0)
        grpUserTrue.map{x=>x.m_total_sessions}.sum should not be (0)
        
        //Record for February month is empty 
        grpUserFalse.filter { x => x.d_period==201602}.count should be (0)
        grpUserTrue.filter { x => x.d_period==201602}.count should be (0)
        
        //Record for week 11 () is empty
        grpUserFalse.filter { x => x.d_period==20167711}.count should be (0)
        grpUserTrue.filter { x => x.d_period==20167711}.count should be (0)
        
        
        //Checking a aggregate value of daily summary of a week to the weekly summary  
        val dailySumm1 = grpUserFalse.filter { x => (x.d_period > 20160103 && x.d_period < 20160111) }
        val weekly1 = grpUserFalse.filter { x => (x.d_period == 2016771) }.first()
        dailySumm1.map(f => f.m_total_sessions).sum should be(weekly1.m_total_sessions)
        dailySumm1.map(f => f.m_total_ts).sum should be(weekly1.m_total_ts)
        
        //grpUserFalse.filter{x=>(x.d_period==20157753)
        
    }
}