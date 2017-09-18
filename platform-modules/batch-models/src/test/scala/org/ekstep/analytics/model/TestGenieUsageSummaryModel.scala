package org.ekstep.analytics.model

import org.ekstep.analytics.framework.RegisteredTag
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.DerivedEvent

class TestGenieUsageSummaryModel extends SparkSpec(null) {

    it should "test the summary for one month of data having only two pre-registered tags" in {

        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE local_content_db.registered_tags");
        }

        val tag1 = RegisteredTag("42d3b7edc2e9b59a286b1956e3cdbc492706ac21", System.currentTimeMillis(), true)
        val tag2 = RegisteredTag("becb887fe82f24c644482eb30041da6d88bd8150", System.currentTimeMillis(), true)
        sc.makeRDD(List(tag1, tag2)).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS)

        val rdd = loadFile[DerivedEvent]("src/test/resources/genie-usage-summary-model/gus_1.log");
        val out = GenieUsageSummaryModel.execute(rdd, None);
        val events = out.collect()

        val tag1Events = events.filter { x => StringUtils.equals(x.dimensions.tag.get, "42d3b7edc2e9b59a286b1956e3cdbc492706ac21") }
        val tag2Events = events.filter { x => StringUtils.equals(x.dimensions.tag.get, "becb887fe82f24c644482eb30041da6d88bd8150") }
        tag1Events.size should be > (0)
        tag2Events.size should be > (0)

        // ACROSS ALL TAG
        val allTagEvents = events.filter { x => StringUtils.equals(x.dimensions.tag.get, "all") }
        allTagEvents.length should be(29)
        val aug30Summ = allTagEvents.filter { x => x.dimensions.period.get == 20160830 }.last
//        aug30Summ.mid should be("75152D5CE321A2438D29CA113C16793A")
        aug30Summ.eid should be("ME_GENIE_USAGE_SUMMARY")

        val aug30EksMap = aug30Summ.edata.eks.asInstanceOf[Map[String, AnyRef]]

        aug30EksMap.get("total_ts").get.asInstanceOf[Double] should be(82349.44)

        val devices = aug30EksMap.get("device_ids").get.asInstanceOf[Array[String]]
        devices.size should be(devices.distinct.size)

        val contents = aug30EksMap.get("contents").get.asInstanceOf[Array[String]]
        contents.size should be(contents.distinct.size)

        aug30EksMap.get("total_sessions").get.asInstanceOf[Long] should be(145)
        aug30EksMap.get("avg_ts_session").get.asInstanceOf[Double] should be(567.93)

        // tag1 (42d3b7edc2e9b59a286b1956e3cdbc492706ac21) Summary

        val tag1Aug22Summ = tag1Events.filter { x => x.dimensions.period.get == 20160822 }.last

//        tag1Aug22Summ.mid should be("1410BCF39B9008BD15F5C67732D2378B")
        tag1Aug22Summ.eid should be("ME_GENIE_USAGE_SUMMARY")

        val tag1Aug22SummEksMap = tag1Aug22Summ.edata.eks.asInstanceOf[Map[String, AnyRef]]
        tag1Aug22SummEksMap.get("total_ts").get.asInstanceOf[Double] should be(31957.45)
        val devicesTag1 = tag1Aug22SummEksMap.get("device_ids").get.asInstanceOf[Array[String]]
        devicesTag1.size should be(devicesTag1.distinct.size)
        val contentsTag1 = tag1Aug22SummEksMap.get("contents").get.asInstanceOf[Array[String]]
        contentsTag1.size should be(contentsTag1.distinct.size)
        tag1Aug22SummEksMap.get("total_sessions").get.asInstanceOf[Long] should be(51)
        tag1Aug22SummEksMap.get("avg_ts_session").get.asInstanceOf[Double] should be(626.62)

        // tag2 (becb887fe82f24c644482eb30041da6d88bd8150) Summary    

        val tag2Aug23Summ = tag2Events.filter { x => x.dimensions.period.get == 20160823 }.last

//        tag2Aug23Summ.mid should be("EEE1325B375251F7B11FA55D71CB5309")
        tag2Aug23Summ.eid should be("ME_GENIE_USAGE_SUMMARY")

        val tag2Aug23SummEksMap = tag2Aug23Summ.edata.eks.asInstanceOf[Map[String, AnyRef]]
        val devicesTag2 = tag2Aug23SummEksMap.get("device_ids").get.asInstanceOf[Array[String]]
        devicesTag2.size should be(devicesTag2.distinct.size)
        val contentsTag2 = tag2Aug23SummEksMap.get("contents").get.asInstanceOf[Array[String]]
        contentsTag2.size should be(contentsTag2.distinct.size)
        tag2Aug23SummEksMap.get("total_sessions").get.asInstanceOf[Long] should be(1)
        tag2Aug23SummEksMap.get("total_ts").get.asInstanceOf[Double] should be(tag2Aug23SummEksMap.get("avg_ts_session").get.asInstanceOf[Double])

        val tagOthers = events.filter { x => StringUtils.equals(x.dimensions.tag.get, "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb") || StringUtils.equals(x.dimensions.tag.get, "c6ed6e6849303c77c0182a282ebf318aad28f8d1") || StringUtils.equals(x.dimensions.tag.get, "e4d7a0063b665b7a718e8f7e4014e59e28642f8c") }
        tagOthers.size should be(0)

        //1375b1d70a66a0f2c22dd1096b98030cb7d9bacb, c6ed6e6849303c77c0182a282ebf318aad28f8d1, e4d7a0063b665b7a718e8f7e4014e59e28642f8c

    }

    it should "test the summary for one month of data having pre-registered tags in last week" in {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE local_content_db.registered_tags");
        }

        val tag1 = RegisteredTag("42d3b7edc2e9b59a286b1956e3cdbc492706acb1", System.currentTimeMillis(), true)
        val tag2 = RegisteredTag("e4d7a0063b665b7a718e8f7e4014e59e28642f8c", System.currentTimeMillis(), true)
        sc.makeRDD(List(tag1, tag2)).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS)

        val rdd = loadFile[DerivedEvent]("src/test/resources/genie-usage-summary-model/gus_2.log");
        val out = GenieUsageSummaryModel.execute(rdd, None);
        val events = out.collect()
        
        val allEventsLastWeek = events.filter { x => x.dimensions.period.get > 20160827 && !StringUtils.equals(x.dimensions.tag.get, "all") }
        allEventsLastWeek.size should be > (0)
        val allEventsBfrLastWeek = events.filter { x => x.dimensions.period.get < 20160827 && !StringUtils.equals(x.dimensions.tag.get, "all") }
        allEventsBfrLastWeek.size should be (0)
        
        val tag1Events = allEventsLastWeek.filter { x => StringUtils.equals(x.dimensions.tag.get, "42d3b7edc2e9b59a286b1956e3cdbc492706acb1") }
        tag1Events.length should be(3)
        val tag1Event = tag1Events.filter { x => x.dimensions.period.get == 20160830 }.last
        
//        tag1Event.mid should be("6D7115F2EA5D40D8EC53D21211DAA2CA")
        tag1Event.eid should be("ME_GENIE_USAGE_SUMMARY")

        val tag1EventEksMap = tag1Event.edata.eks.asInstanceOf[Map[String, AnyRef]]

        tag1EventEksMap.get("total_ts").get.asInstanceOf[Double] should be(78939.37)

        val devices = tag1EventEksMap.get("device_ids").get.asInstanceOf[Array[String]]
        devices.size should be(devices.distinct.size)

        val contents =  tag1EventEksMap.get("contents").get.asInstanceOf[Array[String]]
        contents.size should be(contents.distinct.size)

        tag1EventEksMap.get("total_sessions").get.asInstanceOf[Long] should be(114)
        tag1EventEksMap.get("avg_ts_session").get.asInstanceOf[Double] should be(692.45)
        
    }
    it should "test the summary for one month of data where last week data is missing" in {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE local_content_db.registered_tags");
        }
        
        val rdd = loadFile[DerivedEvent]("src/test/resources/genie-usage-summary-model/gus_3.log");
        val out = GenieUsageSummaryModel.execute(rdd, None);
        val events = out.collect()
        
        val allEventsLastWeek = events.filter { x => x.dimensions.period.get > 20160827 }
        allEventsLastWeek.size should be (0)
        val allEventsBfrLastWeek = events.filter { x => x.dimensions.period.get < 20160827 }
        allEventsBfrLastWeek.size should be > (0)
        
    }
}