package org.ekstep.analytics.job.updater
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.Dispatcher
import com.pygmalios.reactiveinflux.spark._
import com.paulgoldbaum.influxdbclient._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ Await, Future }
import scala.concurrent.duration._
import java.util.Calendar
import java.sql.Date
import org.joda.time.DateTime
import org.ekstep.analytics.util.Constants

class TestInfluxDBUpdater extends SparkSpec(null) {
    "InfluxDBUpdater" should "execute the job" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/genie-usage-updater/gus_1.log"))))), None, None, "org.ekstep.analytics.updater.InfluxDBUpdater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestInfluxDBUpdater"), Option(false))
        InfluxDBUpdater.main(JSONUtils.serialize(config))(Option(sc));
        val influxdb = InfluxDB.connect(Constants.LOCAL_HOST, Constants.INFLUX_DB_PORT)
        val database = influxdb.selectDatabase(Constants.INFLUX_DB_NAME)
        val result = database.query("SELECT * FROM genie_metrics")
        val res = Await.result(result, 5 seconds)
        res.series.head.columns.size should be(5)
    }
}