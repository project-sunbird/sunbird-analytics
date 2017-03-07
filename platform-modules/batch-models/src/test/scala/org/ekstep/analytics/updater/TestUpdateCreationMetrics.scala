package org.ekstep.analytics.updater

import java.net.URI
import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.model.SparkSpec
import com.pygmalios.reactiveinflux._

class TestUpdateCreationMetrics extends SparkSpec(null) {
    "UpdateCreationMetrics" should "push data into influxDB" in {
        val rdd = loadFile[CreationMetrics]("src/test/resources/influxDB-updater/concepts.json");
        CreationMetricsUpdater.execute(rdd, None);
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.localhost")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT * FROM concept_metrics")
            queryResult.result.isEmpty should be(false)
        }
    }

    it should "check count of coulmns in influxdb table" in {
        val rdd = loadFile[CreationMetrics]("src/test/resources/influxDB-updater/template.json");
        CreationMetricsUpdater.execute(rdd, None);
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.localhost")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT * FROM template_metrics")
            queryResult.result.singleSeries.columns.size should be(5)
        }
    }

    it should "validate table name" in {
        val rdd = loadFile[CreationMetrics]("src/test/resources/influxDB-updater/template.json");
        CreationMetricsUpdater.execute(rdd, None);
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.localhost")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT * FROM template_metrics")
            queryResult.result.singleSeries.name should be("template_metrics")
        }
    }

    it should "generate first coulmn as time " in {
        val rdd = loadFile[CreationMetrics]("src/test/resources/influxDB-updater/template.json");
        CreationMetricsUpdater.execute(rdd, None);
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.localhost")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT * FROM template_metrics")
            queryResult.result.singleSeries.columns(0) should be("time")
        }
    }
    
    it should "validate items for concept_id " in {
        implicit val awaitAtMost = 10.seconds
        syncInfluxDb(new URI(AppConf.getConfig("reactiveinflux.localhost")), AppConf.getConfig("reactiveinflux.database")) { db =>
            val queryResult = db.query("SELECT items FROM concept_metrics where concept_id = 'id7'")
            queryResult.rows.map { x => x.mkString.split(",")(1).trim() } should be(List("22"))
        }
    }
}