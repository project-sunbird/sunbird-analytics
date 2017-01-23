package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.Response
import org.apache.spark.SparkContext
import org.ekstep.analytics.api.util.JSONUtils
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.api.ContentUsageSummaryFact
import org.ekstep.analytics.api.Constants
import com.datastax.spark.connector.cql.CassandraConnector

case class ServiceHealthReport(name: String, healthy: Boolean, message: Option[String] = None)

object HealthCheckAPIService {

    def getHealthStatus()(implicit sc: SparkContext): String = {

        val checks = getChecks()
        val healthy = checks.forall { x => x.healthy == true }
        val result = Map[String, AnyRef](
            "name" -> "analytics-platform-api",
            "healthy" -> Boolean.box(healthy),
            "checks" -> checks);
        val response = CommonUtil.OK("ekstep.analytics-api.health", result)
        JSONUtils.serialize(response);
    }

    private def checkCassandraConnection()(implicit sc: SparkContext): Boolean = {
        try {
            val connector = CassandraConnector(sc.getConf);
            val session = connector.openSession();
            session.close();
            true;
        } catch {
            // $COVERAGE-OFF$ Disabling scoverage as the below code cannot be covered
            // TODO: Need to get confirmation from amit.
            case ex: Exception =>
                false
            // $COVERAGE-ON$    
        }
    }

    private def getChecks()(implicit sc: SparkContext): Array[ServiceHealthReport] = {
        try {
            val nums = Array(10, 5, 18, 4, 8, 56)
            val rdd = sc.parallelize(nums)
            rdd.sortBy(f => f).collect
            val sparkReport = ServiceHealthReport("Spark Cluster", true);
            val cassReport = ServiceHealthReport("Cassandra Database", checkCassandraConnection());
            Array(sparkReport, cassReport);
        } catch {
            // $COVERAGE-OFF$ Disabling scoverage as the below code cannot be covered
            // TODO: Need to get confirmation from amit.
            case ex: Exception =>
                val sparkReport = ServiceHealthReport("Spark Cluster", false, Option(ex.getMessage));
                val cassReport = ServiceHealthReport("Cassandra Database", false, Option("Unknown.... because of Spark Cluster is not up"));
                Array(sparkReport, cassReport);
            // $COVERAGE-ON$
        }
    }
    
    def main(args: Array[String]): Unit = {
        implicit val sc = CommonUtil.getSparkContext(10, "Test");
        println(getHealthStatus);
    }
}