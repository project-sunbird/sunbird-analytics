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
import scala.concurrent.duration._
import com.datastax.spark.connector.cql.CassandraConnector
import com.paulgoldbaum.influxdbclient._

class TestCreationMetricsUpdater extends SparkSpec(null) {
    "CreationMetricsUpdater" should "execute the job" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/influxDB-updater/item-metrics.json"))))), None, None, "org.ekstep.analytics.updater.ConsumptionMetricsUpdater", Option(Map("periodType" -> "ALL", "periodUpTo" -> 100.asInstanceOf[AnyRef])), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("Consumption Metrics Updater"), Option(false))
        val strConfig = JSONUtils.serialize(config);
        println(strConfig);
        CreationMetricsUpdater.main(strConfig)(Option(sc));
    }
    it should "store items into InfluxDB" in {
        val influxdb = InfluxDB.connect(AppConf.getConfig("reactiveinflux.host"), AppConf.getConfig("reactiveinflux.port").toInt)
        val database = influxdb.selectDatabase(AppConf.getConfig("reactiveinflux.database"))
        val result = database.query("SELECT * FROM item_metrics")
        val res = Await.result(result, 5 seconds)
        res.series.head.columns.size should be(5)
    }
}