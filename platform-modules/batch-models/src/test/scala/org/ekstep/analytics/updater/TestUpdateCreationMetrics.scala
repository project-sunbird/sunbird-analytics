package org.ekstep.analytics.updater

import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.DerivedEvent
import scala.concurrent.Await
import org.ekstep.analytics.util.Constants
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.updater.CreationMerics
import scala.concurrent.duration._
import com.datastax.spark.connector.cql.CassandraConnector
import com.paulgoldbaum.influxdbclient._

class TestUpdateCreationMetrics extends SparkSpec(null) {
    "UpdateCreationMetrics" should "push data into influxDB" in {
        val rdd = loadFile[CreationMerics]("src/test/resources/influxDB-updater/asset.json");
        val rdd2 = CreationMetricsUpdater.execute(rdd, None);
        val influxdb = InfluxDB.connect(AppConf.getConfig("reactiveinflux.host"), AppConf.getConfig("reactiveinflux.port").toInt)
        val database = influxdb.selectDatabase(AppConf.getConfig("reactiveinflux.database"))
        val result = database.query("SELECT * FROM asset")
        val res = Await.result(result, 5 seconds)
        res.series.head.columns.size should be(4)
    }
}