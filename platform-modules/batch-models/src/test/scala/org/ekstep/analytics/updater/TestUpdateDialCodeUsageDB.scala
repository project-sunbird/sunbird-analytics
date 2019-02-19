package org.ekstep.analytics.updater

import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.DerivedEvent
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.util.Constants

class TestUpdateDialCodeUsageDB extends SparkSpec( file = null) {

  override def beforeAll() {
    super.beforeAll()
    val connector = CassandraConnector(sc.getConf)
    val session = connector.openSession()
    session.execute("TRUNCATE " + Constants.PLATFORM_KEY_SPACE_NAME + "." + Constants.DIALCODE_USAGE_METRICS_TABLE)
  }
  var result: GraphUpdateEvent = _

  "UpdateDialcodeUsageDB" should "store data in dialcode_usage_metrics" in {
    val rdd = loadFile[DerivedEvent]("src/test/resources/dialcode-usage-updater/dialcode-usage-summary.log")
    result = UpdateDialcodeUsageDB.execute(rdd, None).first()

    val dayRecord = sc.cassandraTable[DialCodeUsage](Constants.PLATFORM_KEY_SPACE_NAME, Constants.DIALCODE_USAGE_METRICS_TABLE).where("period=?", 20181107).first()
    dayRecord.first_scan should be(1540469152000L)
    dayRecord.last_scan should be(1541541952000L)
    dayRecord.total_dial_scans_local should be(25)
    dayRecord.average_scans_per_day should be(2)

    val weekRecord = sc.cassandraTable[DialCodeUsage](Constants.PLATFORM_KEY_SPACE_NAME, Constants.DIALCODE_USAGE_METRICS_TABLE).where("period=?", 2018745).first()
    weekRecord.first_scan should be(1540469152000L)
    weekRecord.last_scan should be(1541541952000L)
    weekRecord.total_dial_scans_local should be(50)
    weekRecord.average_scans_per_day should be(4)

    val monthRecord = sc.cassandraTable[DialCodeUsage](Constants.PLATFORM_KEY_SPACE_NAME, Constants.DIALCODE_USAGE_METRICS_TABLE).where("period=?", 201811).first()
    monthRecord.first_scan should be(1540469152000L)
    monthRecord.last_scan should be(1541541952000L)
    monthRecord.total_dial_scans_local should be(60)
    monthRecord.average_scans_per_day should be(5)

    val cumulativeRecord = sc.cassandraTable[DialCodeUsage](Constants.PLATFORM_KEY_SPACE_NAME, Constants.DIALCODE_USAGE_METRICS_TABLE).where("period=?", 0).first()
    cumulativeRecord.first_scan should be(1540469152000L)
    cumulativeRecord.last_scan should be(1541541952000L)
    cumulativeRecord.total_dial_scans_local should be(80)
    cumulativeRecord.average_scans_per_day should be(6)
  }

  it should "update the DB if first_scan is less than the current first_scan" in {

    val rdd = loadFile[DerivedEvent]("src/test/resources/dialcode-usage-updater/dialcode-usage-summary1.log")
    UpdateDialcodeUsageDB.execute(rdd, None)

    val newValue = sc.cassandraTable[DialCodeUsage](Constants.PLATFORM_KEY_SPACE_NAME, Constants.DIALCODE_USAGE_METRICS_TABLE).where("period=?", 20181107).first()
    newValue.first_scan should be(1540469130000L)
  }

  it should "update the DB if last_scan is greater than the current last_scan" in {

    val newValue = sc.cassandraTable[DialCodeUsage](Constants.PLATFORM_KEY_SPACE_NAME, Constants.DIALCODE_USAGE_METRICS_TABLE).where("period=?", 20181107).first()
    newValue.last_scan should be(1541542052000L)
  }

  it should "change the average_scans_per_day value according to the day difference between first_scan and last_scan " in {

    val withoutDifference = sc.cassandraTable[DialCodeUsage](Constants.PLATFORM_KEY_SPACE_NAME, Constants.DIALCODE_USAGE_METRICS_TABLE).where("period=?", 20181026).first()
    withoutDifference.average_scans_per_day should be(withoutDifference.total_dial_scans_local)
    val withDifference = sc.cassandraTable[DialCodeUsage](Constants.PLATFORM_KEY_SPACE_NAME, Constants.DIALCODE_USAGE_METRICS_TABLE).where("period=?", 20181107).first()
    withDifference.average_scans_per_day should not be(withDifference.total_dial_scans_local)
  }

  it should "Return GraphUpdateEvent with proper values" in {
    result.nodeUniqueId should be("QR1234")
    result.transactionData("properties")("total_dial_scans_local")("nv") should be(25)
    result.transactionData("properties")("total_dial_scans_global")("nv") should be(25)
    result.transactionData("properties")("first_scan")("nv") should be(1540469152000L)
    result.transactionData("properties")("last_scan")("nv") should be(1541456052000L)
    result.transactionData("properties")("average_scans_per_day")("nv") should be(2)
    result.nodeType should be("DIALCODE_METRICS")
    result.audit should be(false)
  }

}
