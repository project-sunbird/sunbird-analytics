package org.ekstep.analytics.updater
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.MeasuredEvent
import org.ekstep.analytics.framework.DerivedEvent
import com.paulgoldbaum.influxdbclient.InfluxDB
import scala.concurrent.Await
import org.ekstep.analytics.util.Constants
import com.paulgoldbaum.influxdbclient._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class TestUpdateInfluxDB extends SparkSpec(null) {
    "UpdateInfluxDB" should "update the influxdb" in {
        val rdd = loadFile[DerivedEvent]("src/test/resources/genie-usage-updater/gus_1.log");
        val rdd2 = UpdateInfluxDB.execute(rdd, None);
        val influxdb = InfluxDB.connect(Constants.LOCAL_HOST, Constants.INFLUX_DB_PORT)
        val database = influxdb.selectDatabase(Constants.INFLUX_DB_NAME)
        val result = database.query("SELECT * FROM genie_metrics")
        val res = Await.result(result, 5 seconds)
        res.series.head.columns(0) should be("time")

    }

}