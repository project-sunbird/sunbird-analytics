package org.ekstep.analytics.model

import org.scalatest.BeforeAndAfterEach
import com.datastax.spark.connector._
import org.joda.time.DateTime
import org.joda.time.DateTimeZone
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.RegisteredTag
import org.ekstep.analytics.framework.V3Event

class TestContentPopularitySummaryModel extends SparkSpec(null) with BeforeAndAfterEach {
	
	val tagList = Array(
				"dff9175fa217e728d86bc1f4d8f818f6d2959303",
				"1375b1d70a66a0f2c22dd1096b98030cb7d9bacb", 
				"2c3e26df61c65c3c5e38c514593a0ee2678b1ffd", 
				"f202014e4a59d9f31e26311575a7c1b5b9eac698",
				"7b4396e2882a0b235ea8557ddf4a473bf7c28e92",
				"7e2882a0b235ea8557ddf4a47343dgdf7c435492",
				"7e2882a0b235ea8557ddf4a47343dgdf7c435493");
	
	override def afterAll() {
		CassandraConnector(sc.getConf).withSessionDo { session =>
			val keySpace = Constants.CONTENT_KEY_SPACE_NAME;
			val table = Constants.REGISTERED_TAGS;
			session.execute(s"TRUNCATE $keySpace.$table");
		}
		super.afterAll();
	}
	
	private def registerTags(tags: Array[String], active: Boolean = true) = {
		val registeredTags = tags.map { x => 
			RegisteredTag(x, System.currentTimeMillis(), active)	
		}.toSeq;
		val rdd = sc.makeRDD(registeredTags);
        rdd.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS);
	}
	
	private def tagEventsLength(measuredEvents: Array[MeasuredEvent], tag: String) : Int = {
		measuredEvents.filter { event => tag.equals(event.dimensions.tag.get) }.length;
	}
	
	override def beforeEach() {
		registerTags(tagList, false);
	}
	
	// TC-1:
	"ContentPopularitySummaryModel" should "generate summaries only for two registered tags when, one week of data where out of five tags two tags are pre-registered on portal" in {
		registerTags(Array("dff9175fa217e728d86bc1f4d8f818f6d2959303", "f202014e4a59d9f31e26311575a7c1b5b9eac698"));
		val rdd = loadFile[V3Event]("src/test/resources/content-popularity-summary/v3/test_data1.log");
		val resultRDD = ContentPopularitySummaryModel.execute(rdd, None);
		val measuredEvents = resultRDD.collect();
		tagEventsLength(measuredEvents, tagList(0)) should be(103);
		tagEventsLength(measuredEvents, tagList(1)) should be(0);
		tagEventsLength(measuredEvents, tagList(2)) should be(0);
		tagEventsLength(measuredEvents, tagList(3)) should be(7);
		tagEventsLength(measuredEvents, tagList(4)) should be(0);
		
	}
	
	// TC-2:
	it should "not generate summaries for tags when, one week of data where out of five tags none of them are pre-registered on portal" in {
		val rdd = loadFile[V3Event]("src/test/resources/content-popularity-summary/v3/test_data2.log");
		val resultRDD = ContentPopularitySummaryModel.execute(rdd, None);
		val measuredEvents = resultRDD.collect();
		tagEventsLength(measuredEvents, tagList(0)) should be(0);
		tagEventsLength(measuredEvents, tagList(1)) should be(0);
		tagEventsLength(measuredEvents, tagList(2)) should be(0);
		tagEventsLength(measuredEvents, tagList(3)) should be(0);
		tagEventsLength(measuredEvents, tagList(4)) should be(0);
	}
	
	// TC-3:
	it should "generate summaries for all tags when, one week of data where all five tags are pre-registered on portal" in {
		registerTags(tagList);
		val rdd = loadFile[V3Event]("src/test/resources/content-popularity-summary/v3/test_data2.log");
		val resultRDD = ContentPopularitySummaryModel.execute(rdd, None);
		val measuredEvents = resultRDD.collect();
		tagEventsLength(measuredEvents, tagList(0)) should be(116);
		tagEventsLength(measuredEvents, tagList(1)) should be(14);
		tagEventsLength(measuredEvents, tagList(2)) should be(2);
		tagEventsLength(measuredEvents, tagList(3)) should be(17);
		tagEventsLength(measuredEvents, tagList(4)) should be(16);
	}
	
	// TC-4:
	it should "not generate summaries for tags which are not exist in data but, pre-registered on portal" in  {
		registerTags(Array(tagList(5), tagList(6)));
		val rdd = loadFile[V3Event]("src/test/resources/content-popularity-summary/v3/test_data1.log");
		val resultRDD = ContentPopularitySummaryModel.execute(rdd, None);
		val measuredEvents = resultRDD.collect();
		tagEventsLength(measuredEvents, tagList(5)) should be(0);
		tagEventsLength(measuredEvents, tagList(6)) should be(0);
	} 
  
}