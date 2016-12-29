import org.specs2.mutable._
import org.specs2.runner._
import org.junit.runner._

import play.api.libs.json._
import play.api.http._
import play.api.test._
import play.api.test.Helpers._
import org.ekstep.analytics.api.util.JSONUtils
import com.datastax.spark.connector.cql.CassandraConnector
import context.Context
import org.ekstep.analytics.api.util.DBUtil
import org.ekstep.analytics.api.JobRequest
import com.datastax.spark.connector._
import org.joda.time.DateTime
import org.ekstep.analytics.api.JobResponse
import scala.concurrent.Await

@RunWith(classOf[JUnitRunner])
class JobsAPISpec extends BaseSpec with Serializable {

    "Job Request API" should {

        CassandraConnector(Context.sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE platform_db.job_request");
        }

        "return error response on invalid request - error response" in new WithApplication {
            val request = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal" },"request": {"filter": {"start_date": "2016-11-20","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]  }}}"""
            val response = post("/dataset/request", request);
            hasClientError(response);
        }

        "return the result - successful response" in new WithApplication {
            val request = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal" },"request": {"filter": {"start_date": "2016-11-03","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]  }}}"""
            val response = post("/dataset/request", request);
            isOK(response);
        }

        "return error response on empty request - error response" in new WithApplication {
            val request = """ {}"""
            val response = post("/dataset/request", request);
            println(response)
            //hasClientError(response);
        }

        "return status as processed when submitted job is processed" in new WithApplication {
            val request = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal1" },"request": {"filter": {"start_date": "2016-11-04","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef7"]  }}}"""
            val response = post("/dataset/request", request);
            val rowRDD = Context.sc.cassandraTable[JobRequest]("platform_db", "job_request");
            val filterRDD = rowRDD.filter { x => (x.client_key.get == "dev-portal1") }
            val rddJobRequest = filterRDD.map { x => JobRequest(x.client_key, x.request_id, x.job_id, Some("PROCESSING"), x.request_data, x.iteration, x.dt_job_submitted, x.location, x.dt_file_created, x.dt_first_event, x.dt_last_event, x.dt_expiration, x.dt_job_processing, x.dt_job_completed, x.input_events, x.output_events, x.file_size, x.latency, x.execution_time, x.err_message, x.stage, x.stage_status) }

            rddJobRequest.saveToCassandra("platform_db", "job_request")

            val request1 = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal1" },"request": {"filter": {"start_date": "2016-11-04","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef7"]  }}}"""
            val response1 = post("/dataset/request", request1);
           // contentAsString(response1) must contain(""""status":"PROCESSING"""")
        }


         "return status as completed when submitted job is completed" in new WithApplication {
            val request = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal2" },"request": {"filter": {"start_date": "2016-11-05","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef8"]  }}}"""
            val response = post("/dataset/request", request);
            val rowRDD = Context.sc.cassandraTable[JobRequest]("platform_db", "job_request");
            val filterRDD = rowRDD.filter { x => (x.client_key.get == "dev-portal2") }
            val rddJobRequest = filterRDD.map { x => JobRequest(x.client_key, x.request_id, x.job_id, Some("COMPLETE"), x.request_data, x.iteration, x.dt_job_submitted, Some("http://"), Some(new org.joda.time.DateTime("2016-12-28")), Some(new org.joda.time.DateTime("2016-12-28")), Some(new org.joda.time.DateTime("2016-12-28")), Some(new org.joda.time.DateTime("2016-12-28")), Some(new org.joda.time.DateTime("2016-12-28")), Some(new org.joda.time.DateTime("2016-12-28")), Some(1000), Some(2000), Some(12333), Some(22345), Some(12345l), x.err_message, x.stage, x.stage_status) }


            rddJobRequest.saveToCassandra("platform_db", "job_request")

            val request1 = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal2" },"request": {"filter": {"start_date": "2016-11-05","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef8"]  }}}"""
            val response1 = post("/dataset/request", request1);


            //contentAsString(response1) must contain(""""status":"COMPLETE"""")
        }


        "return status as FAILED when job got FAILED" in new WithApplication {


            val request = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal3" },"request": {"filter": {"start_date": "2016-11-06","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef9"]  }}}"""
            val response = post("/dataset/request", request);
            val rowRDD = Context.sc.cassandraTable[JobRequest]("platform_db", "job_request");
            val filterRDD = rowRDD.filter { x => (x.client_key.get == "dev-portal3") }
            val rddJobRequest = filterRDD.map { x => JobRequest(x.client_key, x.request_id, x.job_id, Some("FAILED"), x.request_data, x.iteration, x.dt_job_submitted, x.location, x.dt_file_created, x.dt_first_event, x.dt_last_event, x.dt_expiration, x.dt_job_processing, x.dt_job_completed, x.input_events, x.output_events, x.file_size, x.latency, x.execution_time, x.err_message, x.stage, x.stage_status) }

            rddJobRequest.saveToCassandra("platform_db", "job_request")

            val request1 = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal3" },"request": {"filter": {"start_date": "2016-11-06","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef9"]  }}}"""
            val response1 = post("/dataset/request", request1);
            import scala.concurrent.Await
            import scala.concurrent.duration.DurationInt

            val data1 = Await.result(response1, 20.seconds);
            println("data1.connection")
            println(data1.connection)
            println("data1.body")
            println(data1.body)
            println("data1.header")
            println(data1.header)

           // contentAsString(response1) must contain(""""status":"FAILED"""")

        }
        /*"return status as FAILED when job got FAILED" in new WithApplication {

            val request = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal4" },"request": {"filter": {"start_date": "2016-11-04","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef10"]  }}}"""
            val response = post("/dataset/request", request);
            val rowRDD = Context.sc.cassandraTable[JobRequest]("platform_db", "job_request");
            val filterRDD = rowRDD.filter { x => (x.client_key.get == "dev-portal4") }
            val rddJobRequest = filterRDD.map { x => JobRequest(x.client_key, x.request_id, x.job_id, Some("RETRY"), x.request_data, x.iteration, x.dt_job_submitted, x.location, x.dt_file_created, x.dt_first_event, x.dt_last_event, x.dt_expiration, x.dt_job_processing, x.dt_job_completed, x.input_events, x.output_events, x.file_size, x.latency, x.execution_time, x.err_message) }

            rddJobRequest.saveToCassandra("platform_db", "job_request")

            val request1 = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal4" },"request": {"filter": {"start_date": "2016-11-04","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef10"]  }}}"""
            val response1 = post("/dataset/request", request1);

            contentAsString(response1) must contain(""""status":"RETRY"""")

        }*/

    }
}