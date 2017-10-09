package org.ekstep.analytics.api.util

import java.lang.reflect.ParameterizedType
import java.lang.reflect.Type

import scala.collection.JavaConverters.iterableAsScalaIterableConverter
import scala.reflect.runtime.universe.MethodSymbol
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.runtime.universe.typeOf
import scala.reflect.runtime.{ currentMirror => m, universe => ru }

import org.ekstep.analytics.api.Constants
import org.ekstep.analytics.api.JobRequest
import org.ekstep.analytics.framework.conf.AppConf
import org.joda.time.DateTime

import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Row
import com.datastax.driver.core.Session
import com.datastax.driver.core.querybuilder.{ QueryBuilder => QB }

import org.ekstep.analytics.framework.util.JobLogger
import akka.actor.Actor
import com.fasterxml.jackson.core.`type`.TypeReference

object DBUtil {

    case class GetJobRequest(requestId: String, clientId: String);
    case class SaveJobRequest(jobRequest: Array[JobRequest]);

    implicit val className = "DBUtil"
    val embeddedCassandra = AppConf.getConfig("cassandra.service.embedded.enable").toBoolean
    val host = AppConf.getConfig("spark.cassandra.connection.host")
    val port = if (embeddedCassandra) AppConf.getConfig("cassandra.service.embedded.connection.port").toInt else 9042
    val cluster = {
        Cluster.builder()
            .addContactPoint(host)
            .withPort(port)
            .build()
    }
    val session = cluster.connect()

    def getJobRequest(requestId: String, clientKey: String): JobRequest = {
        val query = QB.select().from(Constants.PLATFORML_DB, Constants.JOB_REQUEST).allowFiltering().where(QB.eq("request_id", requestId)).and(QB.eq("client_key", clientKey))
        val resultSet = session.execute(query)
        val job = resultSet.asScala.map(row => rowToCaseClass(row)).toArray
        if (job.isEmpty) null; else job.last;
    }

    def getJobRequestList(clientKey: String): Array[JobRequest] = {
        val query = QB.select().from(Constants.PLATFORML_DB, Constants.JOB_REQUEST).allowFiltering().where(QB.eq("client_key", clientKey))
        val job = session.execute(query)
        job.asScala.map(row => rowToCaseClass(row)).toArray.sortWith(_.dt_job_submitted.get.getMillis > _.dt_job_submitted.get.getMillis)
    }

    def saveJobRequest(jobRequests: Array[JobRequest]) = {
        jobRequests.map { jobRequest =>
            val query = QB.insertInto(Constants.PLATFORML_DB, Constants.JOB_REQUEST).value("client_key", jobRequest.client_key.get).value("request_id", jobRequest.request_id.get).value("job_id", jobRequest.job_id.getOrElse(null)).value("status", jobRequest.status.getOrElse()).value("request_data", jobRequest.request_data.getOrElse(null)).value("iteration", jobRequest.iteration.getOrElse(0)).value("dt_job_submitted", setDateColumn(jobRequest.dt_job_submitted).getOrElse(null)).value("location", jobRequest.location.getOrElse(null))
                .value("dt_file_created", setDateColumn(jobRequest.dt_file_created).getOrElse(null)).value("dt_first_event", setDateColumn(jobRequest.dt_first_event).getOrElse(null)).value("dt_last_event", setDateColumn(jobRequest.dt_last_event).getOrElse(null)).value("dt_expiration", setDateColumn(jobRequest.dt_expiration).getOrElse(null)).value("dt_job_processing", setDateColumn(jobRequest.dt_job_processing).getOrElse(null)).value("dt_job_completed", setDateColumn(jobRequest.dt_job_completed).getOrElse(null)).value("input_events", jobRequest.input_events.getOrElse(0))
                .value("output_events", jobRequest.output_events.getOrElse(0)).value("file_size", jobRequest.file_size.getOrElse(0L)).value("latency", jobRequest.latency.getOrElse(0)).value("execution_time", jobRequest.execution_time.getOrElse(0L)).value("err_message", jobRequest.err_message.getOrElse(null)).value("stage", jobRequest.stage.getOrElse(null)).value("stage_status", jobRequest.stage_status.getOrElse(null))
            session.execute(query)
        }
    }

    def getDateColumn(row: Row, column: String): Option[DateTime] = if (null == row.getObject(column)) None else Option(new DateTime(row.getTimestamp("dt_job_submitted")))

    def setDateColumn(date: Option[DateTime]): Option[Long] = {
        val timestamp = date.getOrElse(null)
        if (null == timestamp) None else Option(timestamp.getMillis())
    }

    def rowToCaseClass(row: Row): JobRequest = {
        JobRequest(Option(row.getString("client_key")), Option(row.getString("request_id")), Option(row.getString("job_id")), Option(row.getString("status")), Option(row.getString("request_data")), Option(row.getInt("iteration")), getDateColumn(row, "dt_job_submitted"), Option(row.getString("location")), getDateColumn(row, "dt_file_created"),
            getDateColumn(row, "dt_first_event"), getDateColumn(row, "dt_last_event"), getDateColumn(row, "dt_expiration"), getDateColumn(row, "dt_job_processing"), getDateColumn(row, "dt_job_completed"), Option(row.getInt("input_events")), Option(row.getInt("output_events")), Option(row.getLong("file_size")),
            Option(row.getInt("latency")), Option(row.getLong("execution_time")), Option(row.getString("err_message")), Option(row.getString("stage")), Option(row.getString("stage_status")))
    }

    sys.ShutdownHookThread {
        session.close()
        JobLogger.log("Closing the cassandra session")
    }

    def checkCassandraConnection(): Boolean = {
        try {
            val cluster = new Cluster.Builder().
                addContactPoints(host).
                withPort(port).build()
            val session: Session = cluster.connect
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
}

class DBUtil extends Actor {
    import DBUtil._;

    def receive = {
        case GetJobRequest(requestId: String, clientId: String) => getJobRequest(requestId, clientId);
        case SaveJobRequest(jobRequest: Array[JobRequest])      => saveJobRequest(jobRequest);
    }
}