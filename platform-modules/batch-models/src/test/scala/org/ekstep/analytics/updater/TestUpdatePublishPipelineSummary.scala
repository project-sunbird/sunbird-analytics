package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.DerivedEvent
import org.joda.time.DateTime
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.util.ItemUsageSummaryFact
import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.commons.lang3.StringUtils
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.Period
import org.joda.time.format.DateTimeFormat

class TestUpdatePublishPipelineSummary extends SparkSpec(null) {

  "UpdatePublishPipelineSummary" should "update the content_publish_fact table for DAY, WEEK, MONTH, YEAR" in {

    CassandraConnector(sc.getConf).withSessionDo { session =>
      session.execute("TRUNCATE creation_metrics_db.content_publish_fact");
    }

    val rdd = loadFile[DerivedEvent]("src/test/resources/pipeline-summary-updater/test_data1.log");
    val rdd2 = UpdatePublishPipelineSummary.execute(rdd, None);

    val rdd3 = loadFile[DerivedEvent]("src/test/resources/pipeline-summary-updater/test_data2.log");
    val rdd4 = UpdatePublishPipelineSummary.execute(rdd3, None);

    val daywiseSummary20170523 = sc.cassandraTable[PublishPipelineSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_PUBLISH_FACT).where("d_period=?", 20170523).collect
    daywiseSummary20170523.find(s => s.`type` == "Content" && s.state == "Draft").get.count should be(5)
    daywiseSummary20170523.find(s => s.`type` == "Content" && s.state == "Review").get.count should be(3)
    daywiseSummary20170523.find(s => s.`type` == "Content" && s.state == "Live").get.count should be(4)
    daywiseSummary20170523.find(s => s.`type` == "Item" && s.state == "Live").get.count should be(3)
    daywiseSummary20170523.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "audio").get.count should be(2)
    daywiseSummary20170523.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "video").get.count should be(1)
    daywiseSummary20170523.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "image").get.count should be(2)
    daywiseSummary20170523.find(s => s.`type` == "Plugin" && s.state == "Live").get.count should be(2)
    daywiseSummary20170523.find(s => s.`type` == "Textbook" && s.state == "Draft").get.count should be(4)
    daywiseSummary20170523.find(s => s.`type` == "Textbook" && s.state == "Live").get.count should be(4)

    val daywiseSummary20170524 = sc.cassandraTable[PublishPipelineSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_PUBLISH_FACT).where("d_period=?", 20170524).collect
    daywiseSummary20170524.find(s => s.`type` == "Content" && s.state == "Draft").get.count should be(23)
    daywiseSummary20170524.find(s => s.`type` == "Content" && s.state == "Review").get.count should be(3)
    daywiseSummary20170524.find(s => s.`type` == "Content" && s.state == "Live").get.count should be(4)
    daywiseSummary20170524.find(s => s.`type` == "Item" && s.state == "Live").get.count should be(3)
    daywiseSummary20170524.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "audio").get.count should be(2)
    daywiseSummary20170524.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "video").get.count should be(100)
    daywiseSummary20170524.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "image").get.count should be(2)
    daywiseSummary20170524.find(s => s.`type` == "Plugin" && s.state == "Live").get.count should be(2)
    daywiseSummary20170524.find(s => s.`type` == "Textbook" && s.state == "Draft").get.count should be(4)
    daywiseSummary20170524.find(s => s.`type` == "Textbook" && s.state == "Live").get.count should be(4)

    val daywiseSummary20170525 = sc.cassandraTable[PublishPipelineSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_PUBLISH_FACT).where("d_period=?", 20170525).collect
    daywiseSummary20170525.find(s => s.`type` == "Content" && s.state == "Draft").get.count should be(10)
    daywiseSummary20170525.find(s => s.`type` == "Content" && s.state == "Review").get.count should be(3)
    daywiseSummary20170525.find(s => s.`type` == "Content" && s.state == "Live").get.count should be(4)
    daywiseSummary20170525.find(s => s.`type` == "Item" && s.state == "Live").get.count should be(3)
    daywiseSummary20170525.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "audio").get.count should be(2)
    daywiseSummary20170525.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "video").get.count should be(200)
    daywiseSummary20170525.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "image").get.count should be(2)
    daywiseSummary20170525.find(s => s.`type` == "Plugin" && s.state == "Live").get.count should be(2)
    daywiseSummary20170525.find(s => s.`type` == "Textbook" && s.state == "Draft").get.count should be(3)
    daywiseSummary20170525.find(s => s.`type` == "Textbook" && s.state == "Live").get.count should be(8)

    val weekwiseSummary = sc.cassandraTable[PublishPipelineSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_PUBLISH_FACT).where("d_period=?", 2017721).collect
    weekwiseSummary.find(s => s.`type` == "Content" && s.state == "Draft").get.count should be(38)
    weekwiseSummary.find(s => s.`type` == "Content" && s.state == "Review").get.count should be(9)
    weekwiseSummary.find(s => s.`type` == "Content" && s.state == "Live").get.count should be(12)
    weekwiseSummary.find(s => s.`type` == "Item" && s.state == "Live").get.count should be(9)
    weekwiseSummary.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "audio").get.count should be(6)
    weekwiseSummary.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "video").get.count should be(301)
    weekwiseSummary.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "image").get.count should be(6)
    weekwiseSummary.find(s => s.`type` == "Plugin" && s.state == "Live").get.count should be(6)
    weekwiseSummary.find(s => s.`type` == "Textbook" && s.state == "Draft").get.count should be(11)
    weekwiseSummary.find(s => s.`type` == "Textbook" && s.state == "Live").get.count should be(16)

    val monthwiseSummary201704 = sc.cassandraTable[PublishPipelineSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_PUBLISH_FACT).where("d_period=?", 201704).collect
    monthwiseSummary201704.find(s => s.`type` == "Content" && s.state == "Draft").get.count should be(10)
    monthwiseSummary201704.find(s => s.`type` == "Content" && s.state == "Review").get.count should be(3)
    monthwiseSummary201704.find(s => s.`type` == "Content" && s.state == "Live").get.count should be(4)
    monthwiseSummary201704.find(s => s.`type` == "Item" && s.state == "Live").get.count should be(3)
    monthwiseSummary201704.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "audio").get.count should be(2)
    monthwiseSummary201704.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "video").get.count should be(22)
    monthwiseSummary201704.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "image").get.count should be(2)
    monthwiseSummary201704.find(s => s.`type` == "Plugin" && s.state == "Live").get.count should be(2)
    monthwiseSummary201704.find(s => s.`type` == "Textbook" && s.state == "Draft").get.count should be(3)
    monthwiseSummary201704.find(s => s.`type` == "Textbook" && s.state == "Live").get.count should be(8)

    //
    val cumulativeSummary = sc.cassandraTable[PublishPipelineSummaryFact](Constants.CREATION_METRICS_KEY_SPACE_NAME, Constants.CONTENT_PUBLISH_FACT).where("d_period=?", 0).collect
    cumulativeSummary.find(s => s.`type` == "Content" && s.state == "Draft").get.count should be(48)
    cumulativeSummary.find(s => s.`type` == "Content" && s.state == "Review").get.count should be(12)
    cumulativeSummary.find(s => s.`type` == "Content" && s.state == "Live").get.count should be(16)
    cumulativeSummary.find(s => s.`type` == "Item" && s.state == "Live").get.count should be(12)
    cumulativeSummary.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "audio").get.count should be(8)
    cumulativeSummary.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "video").get.count should be(323)
    cumulativeSummary.find(s => s.`type` == "Asset" && s.state == "Live" && s.subtype == "image").get.count should be(8)
    cumulativeSummary.find(s => s.`type` == "Plugin" && s.state == "Live").get.count should be(8)
    cumulativeSummary.find(s => s.`type` == "Textbook" && s.state == "Draft").get.count should be(14)
    cumulativeSummary.find(s => s.`type` == "Textbook" && s.state == "Live").get.count should be(24)
  }
}