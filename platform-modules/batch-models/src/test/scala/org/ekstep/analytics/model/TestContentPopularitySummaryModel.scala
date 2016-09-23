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

class TestContentPopularitySummaryModel extends SparkSpec(null) with BeforeAndAfterEach {
	
	val tagList = Array(
				"dff9175fa217e728d86bc1f4d8f818f6d2959303",
				"1375b1d70a66a0f2c22dd1096b98030cb7d9bacb", 
				"2c3e26df61c65c3c5e38c514593a0ee2678b1ffd", 
				"6c3791818e80b9d05fb975da1e972431d9f8c2a6",
				"7b4396e2882a0b235ea8557ddf4a473bf7c28e92");
	
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
	
	"ContentPopularitySummaryModel" should "not generate summaries for tags" in {
		val rdd = loadFile[Event]("src/test/resources/content-popularity-summary/test_data.log");
		val resultRDD = ContentPopularitySummaryModel.execute(rdd, None);
		val measuredEvents = resultRDD.collect();
		tagEventsLength(measuredEvents, tagList(0)) should be(0);
		tagEventsLength(measuredEvents, tagList(1)) should be(0);
		tagEventsLength(measuredEvents, tagList(2)) should be(0);
		tagEventsLength(measuredEvents, tagList(3)) should be(0);
		tagEventsLength(measuredEvents, tagList(4)) should be(0);
	}
	
	it should "generate summaries only for two registered tags" in {
		registerTags(Array("dff9175fa217e728d86bc1f4d8f818f6d2959303", "1375b1d70a66a0f2c22dd1096b98030cb7d9bacb"));
		val rdd = loadFile[Event]("src/test/resources/content-popularity-summary/test_data.log");
		val resultRDD = ContentPopularitySummaryModel.execute(rdd, None);
		val measuredEvents = resultRDD.collect();
		tagEventsLength(measuredEvents, tagList(0)) should be(116);
		tagEventsLength(measuredEvents, tagList(1)) should be(14);
		tagEventsLength(measuredEvents, tagList(2)) should be(0);
		tagEventsLength(measuredEvents, tagList(3)) should be(0);
		tagEventsLength(measuredEvents, tagList(4)) should be(0);
	}
	
	it should "generate summaries for all tags" in {
		registerTags(tagList);
		val rdd = loadFile[Event]("src/test/resources/content-popularity-summary/test_data.log");
		val resultRDD = ContentPopularitySummaryModel.execute(rdd, None);
		val measuredEvents = resultRDD.collect();
		tagEventsLength(measuredEvents, tagList(0)) should be(116);
		tagEventsLength(measuredEvents, tagList(1)) should be(14);
		tagEventsLength(measuredEvents, tagList(2)) should be(2);
		tagEventsLength(measuredEvents, tagList(3)) should be(14);
		tagEventsLength(measuredEvents, tagList(4)) should be(16);
	}
  
}