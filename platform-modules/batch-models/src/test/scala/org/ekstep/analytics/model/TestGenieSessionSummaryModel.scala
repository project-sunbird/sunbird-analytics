package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.OutputDispatcher
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.updater.LearnerProfile
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector

class TestGenieSessionSummaryModel extends SparkSpec(null) {

    "GenieSessionSummaryModel" should "generate content summary events" in {

        val sampleSumm = LearnerProfile("6eddac9b-ccdb-47aa-be06-a44b06f7fe62", "Genie", "Ekstep", "5c7567479e8515e740eaa2d21157f610bf057831", Option("male"), Option("en"), None, 2, 7, 2009, false, true, None, None)
        val sampleRDD = sc.parallelize(Array(sampleSumm));
        sampleRDD.saveToCassandra(Constants.KEY_SPACE_NAME, Constants.LEARNER_PROFILE_TABLE, SomeColumns("learner_id", "app_id", "channel_id", "did", "gender", "language", "loc", "standard", "age", "year_of_birth", "group_user", "anonymous_user", "created_date", "updated_date"));

        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/test-data1.log");
        val rdd2 = GenieSessionSummaryModel.execute(rdd, None);
        val events = rdd2.collect
        events.size should be(63)

        // check the number of events where timeSpent==0 

        val zeroTimeSpentGS = events.map { x => x.edata.eks.asInstanceOf[Map[String, AnyRef]].get("timeSpent").get.asInstanceOf[Double] }.filter { x => 0 == x }
        zeroTimeSpentGS.size should be(8)

        val event2 = events.filter { x => x.mid.equals("74A610217F317A24C5076698B01A32DF") }.last

        event2.dimensions.did.get should be("3fdf3dfc1f004ffa3762fdadded8f44208c8d06c")
        event2.dimensions.group_user.get should be(false)
        event2.dimensions.anonymous_user.get should be(false)
        val eksMap2 = event2.edata.eks.asInstanceOf[Map[String, AnyRef]]

        eksMap2.get("timeSpent").get.asInstanceOf[Double] should be(1)
        eksMap2.get("time_stamp").get.asInstanceOf[Number].longValue() should be(1457787921000l)
        eksMap2.get("contentCount").get.asInstanceOf[Int] should be(0)

        CassandraConnector(sc.getConf).withSessionDo { session =>
            val query = "DELETE FROM " + Constants.KEY_SPACE_NAME + "." + Constants.LEARNER_PROFILE_TABLE + " where learner_id='6eddac9b-ccdb-47aa-be06-a44b06f7fe62'"
            session.execute(query);
        }
    }

    it should "generate the genie summary of the input data where some of the events generated after idle time" in {

        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/test-data2.log")
        val rdd2 = GenieSessionSummaryModel.execute(rdd, None);
        val events = rdd2.collect
        events.size should be(5)

        val event2 = events.filter { x => x.dimensions.did.get.equals("5c7567479e8515e740eaa2d21157f610bf057831") }.head
        event2.dimensions.did.get should be("5c7567479e8515e740eaa2d21157f610bf057831")
        event2.dimensions.group_user.get should be(false)
        event2.dimensions.anonymous_user.get should be(false)

        event2.edata.eks.asInstanceOf[Map[String, AnyRef]].get("contentCount").get.asInstanceOf[Int] should be(0)
    }

    // test cases 
    it should "generate the genie summary from the input events with time difference less than idle time (30 mins)" in {

        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/session/test-data3.log")
        val rdd2 = GenieSessionSummaryModel.execute(rdd, None);
        val events = rdd2.collect
        events.size should be(1)

        val event = events.last
        val eksMap = event.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap.get("timeSpent").get.asInstanceOf[Double] should not be (0)
    }

    it should "generate the genie summary from the input events with time difference more than the idle time (30 mins)" in {

        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/session/test-data4.log")
        val rdd2 = GenieSessionSummaryModel.execute(rdd, None);

        val events = rdd2.collect
        events.size should be(2)

        val event1 = events(0)
        val eksMap1 = event1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap1.get("timeSpent").get.asInstanceOf[Double] should be(0)

        val event2 = events.last
        val eksMap2 = event2.edata.eks.asInstanceOf[Map[String, AnyRef]]
        eksMap2.get("timeSpent").get.asInstanceOf[Double] should be(0)
    }

    it should "generate the genie summary from the input of ten events where the time diff between 9th and 10th event is more than idle time (30 mins)" in {

        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/test-data5.log")
        val rdd2 = GenieSessionSummaryModel.execute(rdd, None);

        val events = rdd2.collect
        events.size should be(2)

        val gsseEvent1 = events(0)
        val gsseEksMap1 = gsseEvent1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        gsseEksMap1.get("timeSpent").get.asInstanceOf[Double] should not be (0)

        val gsseEvent2 = events.last
        val gsseEksMap2 = gsseEvent2.edata.eks.asInstanceOf[Map[String, AnyRef]]
        gsseEksMap2.get("timeSpent").get.asInstanceOf[Double] should be(0)
    }

    it should "generate the genie summary from the input of N events where the time diff between (x)th and (x+1)th event is more than idle time (30 mins), (where N-1 > x)" in {
        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/test-data6.log")
        val rdd2 = GenieSessionSummaryModel.execute(rdd, None);

        val events = rdd2.collect
        events.size should be(2)

        val gsseEvent1 = events(0)
        val gsseEksMap1 = gsseEvent1.edata.eks.asInstanceOf[Map[String, AnyRef]]
        gsseEksMap1.get("timeSpent").get.asInstanceOf[Double] should be > (0d)

        val gsseEvent2 = events.last
        val gsseEksMap2 = gsseEvent2.edata.eks.asInstanceOf[Map[String, AnyRef]]
        gsseEksMap2.get("timeSpent").get.asInstanceOf[Double] should be > (0d)
    }

    it should "generate the genie summary from the input of N events of multiple genie session where the time diff between each event is less than idle time (30 mins)" in {

        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/test-data7.log")
        val rdd2 = GenieSessionSummaryModel.execute(rdd, None);

        val events = rdd2.collect
        events.size should be(4)

        for (e <- events) {
            val gseEvent2 = e
            val gseEksMap2 = gseEvent2.edata.eks.asInstanceOf[Map[String, AnyRef]]
            gseEksMap2.get("timeSpent").get.asInstanceOf[Double] should be > (0d)
        }
    }

    it should "generate the genie summary from the input of (N + M) events of two session where the time diff between (x)th and (x + 1)th event is more than idle time (30 min), (where N-1 > x)" in {
        val rdd = loadFile[Event]("src/test/resources/genie-usage-summary/test-data8.log")
        val rdd2 = GenieSessionSummaryModel.execute(rdd, None);

        val events = rdd2.collect
        events.size should be(3)

        for (e <- events) {
            val gseEvent2 = e
            val gseEksMap2 = gseEvent2.edata.eks.asInstanceOf[Map[String, AnyRef]]
            gseEksMap2.get("timeSpent").get.asInstanceOf[Double] should be > (0d)
        }
    }

}