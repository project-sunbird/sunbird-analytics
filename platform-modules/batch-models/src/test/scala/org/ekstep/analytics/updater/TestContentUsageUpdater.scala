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
import org.ekstep.analytics.framework.Dispatcher

class TestContentUsageUpdater extends SparkSpec(null) {

    "Content Usage Updater" should "update the content usage updater db and check the updated fields" in {

        
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE content_db.content_usage_summary_fact");
        }
        
        val sampleSumm = ContentUsageSummaryFact("org.ekstep.story.hi.vayu", 0, false, "Story", "application/vnd.ekstep.ecml-archive", new DateTime(1464762060000L), new DateTime(1464762060000L), 19.96d, 2, 9.98d, 7, 21.04d, None, None)
        val sampleRDD = sc.parallelize(Array(sampleSumm));
        sampleRDD.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT)
        
        val rdd = loadFile[MeasuredEvent]("src/test/resources/content-usage-updater/content_usage_updater.log");
        val rdd2 = ContentUsageUpdater.execute(rdd, None);
        val updatedSumm = sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_USAGE_SUMMARY_FACT).where("d_content_id=?", "org.ekstep.story.hi.vayu").where("d_period=?", 0).first
        updatedSumm.m_total_ts should be(39.92)
        updatedSumm.m_avg_interactions_min should be(21.04d)
        updatedSumm.m_total_interactions should be(14)
        updatedSumm.m_total_sessions should be(4)
    }

    it should "update content_usage_summary_fact table for the content 'org.ekstep.delta' and pass some checks" in {

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
        grpUserTrue.map { x => x.m_total_ts }.sum should be(0)
        grpUserTrue.map { x => x.m_total_sessions }.sum should not be (0)

        //Record for February month is empty 
        grpUserFalse.filter { x => x.d_period == 201602 }.count should be(0)
        grpUserTrue.filter { x => x.d_period == 201602 }.count should be(0)

        //Record for week10(2016-03-07 to 2016-03-13) and week11(2016-03-14 to 2016-03-20) is empty
        grpUserFalse.filter { x => x.d_period == 20167710 }.count should be(0)
        grpUserFalse.filter { x => x.d_period == 20167711 }.count should be(0)

        //Checking a aggregate value of daily summary of a week to the weekly summary  
        val dailySumm1 = grpUserFalse.filter { x => (x.d_period > 20160103 && x.d_period < 20160111) }
        val weekly1 = grpUserFalse.filter { x => (x.d_period == 20167701) }.first()
        dailySumm1.map(f => f.m_total_sessions).sum should be(weekly1.m_total_sessions)
        dailySumm1.map(f => f.m_total_ts).sum should be(weekly1.m_total_ts)

        //total count of monthly summaries
        val allmonth = grpUserFalse.filter { x => (x.d_period > 201512 && x.d_period < 201612) }
        allmonth.collect.size should be(4)

        // total count of weekly summaries
        val allweek1 = grpUserFalse.filter { x => (x.d_period >= 20167701 && x.d_period <= 20167709) }.collect
        val allweek2 = grpUserFalse.filter { x => (x.d_period > 20167710 && x.d_period < 20167730) }.collect
        (allweek1.size + allweek2.size + 1) should be(15)

        //five daily summaries are missing from week53
        val dailySumm53 = grpUserFalse.filter { x => (x.d_period > 20151227 && x.d_period < 20160104 && x.d_period != 20157753) }
        dailySumm53.collect.size should be(2)

        val weekly53 = grpUserFalse.filter { x => (x.d_period == 20157753) }.first()

        dailySumm53.map(f => f.m_total_sessions).sum should be(weekly53.m_total_sessions)
        dailySumm53.map(f => f.m_total_ts).sum should be(weekly53.m_total_ts)

        val cumulative = grpUserFalse.filter { x => (x.d_period == 0) }.first()
        cumulative.m_total_ts should be(41273)
        cumulative.m_total_sessions should be(282)

        val janMonth2016 = grpUserFalse.filter { x => (x.d_period == 201601) }.first()
        janMonth2016.m_total_ts should be(22630)
        janMonth2016.m_total_sessions should be(162)

    }
}