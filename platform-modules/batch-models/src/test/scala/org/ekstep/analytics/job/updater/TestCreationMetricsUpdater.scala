package org.ekstep.analytics.job.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.util.Constants
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import java.net.URI
import com.pygmalios.reactiveinflux._
import org.joda.time.DateTime
import scala.concurrent.duration._

class TestCreationMetricsUpdater extends SparkSpec(null) {

    val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/influxDB-updater/template.json")), Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/influxDB-updater/asset.json"))))), None, None, "org.ekstep.analytics.updater.ConsumptionMetricsUpdater", Option(Map("periodType" -> "ALL", "periodUpTo" -> 100.asInstanceOf[AnyRef])), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("Consumption Metrics Updater"), Option(false))
    val strConfig = JSONUtils.serialize(config);
    CreationMetricsModelUpdater.main(strConfig)(Option(sc));
    
    "CreationMetricsUpdater" should "push data into influxDB" in {
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.localhost")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT * FROM template_metrics")
            queryResult.result.isEmpty should be(false)
        }
    }

    it should "check count of coulmns in influxdb table" in {
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.localhost")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT * FROM template_metrics")
            queryResult.result.singleSeries.columns.size should be(5)
        }
    }

    it should "generate first coulmn as time " in {
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.localhost")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT * FROM template_metrics")
            queryResult.result.singleSeries.columns(0) should be("time")
        }
    }

    it should "validate contents for template_id " in {
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.localhost")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT contents FROM template_metrics where template_id = 'id6' ")
            queryResult.rows.map { x => x.mkString.split(",")(1).trim() } should be(List("2"))
        }
    }
}