package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.RegisteredTag
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.util.ContentPopularitySummaryFact
import org.joda.time.DateTime

class TestUpdateContentPopularityDB extends SparkSpec(null) {
  
  override def beforeAll() {
        super.beforeAll()
        val tag1 = RegisteredTag("1375b1d70a66a0f2c22dd1096b98030cb7d9bacb", System.currentTimeMillis(), true)
        sc.makeRDD(Seq(tag1)).saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.REGISTERED_TAGS)
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE local_content_db.content_popularity_summary_fact");
        }
    }
  
  override def afterAll() {
        CassandraConnector(sc.getConf).withSessionDo { session =>
            session.execute("DELETE FROM local_content_db.registered_tags WHERE tag_id='1375b1d70a66a0f2c22dd1096b98030cb7d9bacb'");
            session.execute("TRUNCATE local_content_db.content_popularity_summary_fact");
        }
        super.afterAll();
    }
  
  it should "update the content popularity updater db and check the updated fields" in {
    
    val rdd = loadFile[DerivedEvent]("src/test/resources/content-popularity-updater/cps.log");
    val rdd1 = loadFile[DerivedEvent]("src/test/resources/content-popularity-updater/2017-01-10-1484116391128.json.gz");
        
    val rdd2 = UpdateContentPopularityDB.execute(rdd, None);
    UpdateContentPopularityDB.execute(rdd1, None);
	  
        // cumulative (period = 0)  
//        val zeroPerContnetSumm = sc.cassandraTable[ContentPopularitySummaryFact](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_POPULARITY_SUMMARY_FACT).where("d_content_id=?", "do_30080822").where("d_period=?", 0).where("d_tag=?", "all").first
  }
}