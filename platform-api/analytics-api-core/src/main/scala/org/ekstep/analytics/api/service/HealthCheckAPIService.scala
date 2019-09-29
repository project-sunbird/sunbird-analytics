package org.ekstep.analytics.api.service

import akka.actor.Actor
import org.ekstep.analytics.api.util._

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

    private def checkRedisConnection(): Boolean = {
        val redis = new RedisUtil()
        redis.checkConnection
    }

    private def checkPostgresConnection(): Boolean = PostgresDBUtil.checkConnection

    private def checkElasticsearchConnection(): Boolean = {
        val es = new ElasticsearchService()
        es.checkConnection
    }

    private def getChecks(): Array[ServiceHealthReport] = {
        try {
            val cassandraStatus = ServiceHealthReport("Cassandra Database", checkCassandraConnection())
            val postgresStatus = ServiceHealthReport("Postgres Database", checkPostgresConnection())
            val redisStatus = ServiceHealthReport("Redis Database", checkRedisConnection())
            val ESStatus = ServiceHealthReport("Elasticsearch Database", checkElasticsearchConnection())
            val DBStatus = ServiceHealthReport("Database Health", true)
            Array(cassandraStatus, postgresStatus, redisStatus, ESStatus, DBStatus);
        } catch {
            // $COVERAGE-OFF$ Disabling scoverage as the below code cannot be covered
            case ex: Exception =>
                val DBStatus = ServiceHealthReport("Database Health", false, Option(ex.getMessage))
                Array(DBStatus)
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