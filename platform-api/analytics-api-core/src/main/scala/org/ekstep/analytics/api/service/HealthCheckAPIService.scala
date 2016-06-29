package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.Response
import org.apache.spark.SparkContext
import org.ekstep.analytics.api.util.JSONUtils
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.api.ContentUsageSummaryFact
import org.ekstep.analytics.api.Constants

case class ServiceHealthReport(name: String, healthy: Boolean, message: Option[String] = None)

object HealthCheckAPIService {

    def getHealthStatus()(implicit sc: SparkContext): String = {

        val checks = getChecks()
        val healthy = nums.forall { x => x.healthy == true }
        val result = Map[String, AnyRef](
            "name" -> "analytics-platform-api",
            "healthy" -> Boolean.box(healthy),
            "checks" -> checks);
        val response = CommonUtil.OK("ekstep.analytics-api.health", result)
        JSONUtils.serialize(response);
    }
    private def getChecks()(implicit sc: SparkContext): Array[ServiceHealthReport] = {
        try {
            val nums = Array(10, 5, 18, 4, 8, 56)
            val rdd = sc.parallelize(nums)
            rdd.sortBy(f => f).collect
            val sparkReport = ServiceHealthReport("Spark Cluster", true);
            var cassReport: ServiceHealthReport = null
            try {
                sc.cassandraTable[ContentUsageSummaryFact](Constants.CONTENT_DB, Constants.CONTENT_SUMMARY_FACT_TABLE).where("d_content_id = ?", "org.ekstep.delta").count
                cassReport = ServiceHealthReport("Cassandra Database", true);
            } catch {
                case ex: Exception =>
                    cassReport = ServiceHealthReport("Cassandra Database", false, Option(ex.getMessage));
            }
            Array(sparkReport, cassReport);
        } catch {
            case ex: Exception =>
                val sparkReport = ServiceHealthReport("Spark Cluster", false, Option(ex.getMessage));
                val cassReport = ServiceHealthReport("Cassandra Database", false, Option("Unknown.... because of Spark Cluster is not up"));
                Array(sparkReport, cassReport);
        }
    }
}