package org.ekstep.analytics.api.service

import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.util.DBUtil
import org.ekstep.analytics.api.util.JSONUtils
import akka.actor.Actor

case class ServiceHealthReport(name: String, healthy: Boolean, message: Option[String] = None)

object HealthCheckAPIService {
  
    case class GetHealthStatus()
  
    def getHealthStatus(): String = {

        val checks = getChecks()
        val healthy = checks.forall { x => x.healthy == true }
        val result = Map[String, AnyRef](
            "name" -> "analytics-platform-api",
            "healthy" -> Boolean.box(healthy),
            "checks" -> checks);
        val response = CommonUtil.OK("ekstep.analytics-api.health", result)
        JSONUtils.serialize(response);
    }

    private def checkCassandraConnection(): Boolean = {
        try {
            DBUtil.checkCassandraConnection
        } catch {
            // $COVERAGE-OFF$ Disabling scoverage as the below code cannot be covered
            // TODO: Need to get confirmation from amit.
            case ex: Exception =>
                false
            // $COVERAGE-ON$    
        }
    }

    private def getChecks(): Array[ServiceHealthReport] = {
        try {
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
    
//    def main(args: Array[String]): Unit = {
//        implicit val sc = CommonUtil.getSparkContext(10, "Test");
//        println(getHealthStatus);
//    }
}

class HealthCheckAPIService extends Actor {
	import HealthCheckAPIService._;

	def receive = {
		case GetHealthStatus => sender() ! getHealthStatus();
	}
}