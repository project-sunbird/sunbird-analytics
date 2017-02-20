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
import scala.concurrent.duration.DurationInt
import scala.concurrent._
import scala.concurrent.duration._
import ExecutionContext.Implicits.global
import scala.concurrent.{ Future }
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{ Failure, Success }
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class JobsAPISpec extends BaseSpec with Serializable {
	
	val url = "/dataset/request/submit";
	
    "Job Request API" should new WithApplication {

        CassandraConnector(Context.sc.getConf).withSessionDo { session =>
            session.execute("TRUNCATE platform_db.job_request");
        }

        "return error response on invalid request - error response" in {
            val request = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal" },"request": {"filter": {"start_date": "2016-11-20","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]  }}}"""
            val response = post(url, request);
            hasClientError(response);
            status(response) must equalTo(OK)
        }

        "return the result - successful response" in {
            val request = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal" },"request": {"filter": {"start_date": "2016-11-03","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]  }}}"""
            val response = post(url, request);
            isOK(response);
            status(response) must equalTo(OK)
        }

        "return error response on empty request - error response" in {
            val request = """ {}"""
            val response = post(url, request);
            //hasClientError(response);
            status(response) must equalTo(OK)
        }

        "return status as processed when submitted job is processed" in {
            val request = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal1" },"request": {"filter": {"start_date": "2016-11-04","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef7"]  }}}"""
            val response = post(url, request);
            val rowRDD = Context.sc.cassandraTable[JobRequest]("platform_db", "job_request");
            val filterRDD = rowRDD.filter { x => (x.client_key.get == "dev-portal1") }
            val rddJobRequest = filterRDD.map { x => JobRequest(x.client_key, x.request_id, x.job_id, Some("PROCESSING"), x.request_data, x.iteration, x.dt_job_submitted, x.location, x.dt_file_created, x.dt_first_event, x.dt_last_event, x.dt_expiration, x.dt_job_processing, x.dt_job_completed, x.input_events, x.output_events, x.file_size, x.latency, x.execution_time, x.err_message, x.stage, x.stage_status) }
            Await.result(response, 300 second)
            rddJobRequest.saveToCassandra("platform_db", "job_request")

            val request1 = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal1" },"request": {"filter": {"start_date": "2016-11-04","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef7"]  }}}"""
            val response1 = post(url, request1);
            Await.result(response1, 300.seconds);
            contentAsString(response1) must contain(""""status":"PROCESSING"""")
        }

        "return status as completed when submitted job is completed" in {
            val request = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal2" },"request": {"filter": {"start_date": "2016-11-05","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef8"]  }}}"""
            val response = post(url, request);
            Await.result(response, 300.seconds);
            val rowRDD = Context.sc.cassandraTable[JobRequest]("platform_db", "job_request");
            val filterRDD = rowRDD.filter { x => (x.client_key.get == "dev-portal2") }
            val rddJobRequest = filterRDD.map { x => JobRequest(x.client_key, x.request_id, x.job_id, Some("COMPLETED"), x.request_data, x.iteration, x.dt_job_submitted, Some("http://"), Some(new org.joda.time.DateTime(DateTime.now())), Some(new org.joda.time.DateTime(DateTime.now())), Some(new org.joda.time.DateTime(DateTime.now())), Some(new org.joda.time.DateTime(DateTime.now().plusDays(2))), Some(new org.joda.time.DateTime(DateTime.now())), Some(new org.joda.time.DateTime(DateTime.now())), Some(1000), Some(2000), Some(12333), Some(22345), Some(12345l), x.err_message, x.stage, x.stage_status) }

            rddJobRequest.saveToCassandra("platform_db", "job_request")

            val request1 = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal2" },"request": {"filter": {"start_date": "2016-11-05","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef8"]  }}}"""
            val response1 = post(url, request1);

            contentAsString(response1) must contain(""""status":"COMPLETED"""")
            status(response) must equalTo(OK)
        }

        "return status as failed when submitted job is failed" in {
            val request = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal4" },"request": {"filter": {"start_date": "2016-11-05","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef10"]  }}}"""
            val response = post(url, request);
            Await.result(response, 300.seconds);

            val rowRDD = Context.sc.cassandraTable[JobRequest]("platform_db", "job_request");
            val filterRDD = rowRDD.filter { x => (x.client_key.get == "dev-portal4") }
            val rddJobRequest = filterRDD.map { x => JobRequest(x.client_key, x.request_id, x.job_id, Some("FAILED"), x.request_data, x.iteration, x.dt_job_submitted, Some("http://"), Some(new org.joda.time.DateTime("2016-12-28")), Some(new org.joda.time.DateTime("2016-12-28")), Some(new org.joda.time.DateTime("2016-12-28")), Some(new org.joda.time.DateTime("2016-12-28")), Some(new org.joda.time.DateTime("2016-12-28")), Some(new org.joda.time.DateTime("2016-12-28")), Some(1000), Some(2000), Some(12333), Some(22345), Some(12345l), x.err_message, x.stage, x.stage_status) }

            rddJobRequest.saveToCassandra("platform_db", "job_request")

            val request1 = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal4" },"request": {"filter": {"start_date": "2016-11-05","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef10"]  }}}"""
            val response1 = post(url, request1);

            Await.result(response1, 300.seconds);
            contentAsString(response1) must contain(""""status":"FAILED"""")
            status(response) must equalTo(OK)
        }

        "return api status report - SUBMITTED response" in {
            val response = route(FakeRequest(GET, "/dataset/request/read/dev-portal/B1FBFDB4C5E2EFB34759532650CC4FEB")).get
            status(response) must equalTo(OK)
            contentAsString(response) must contain(""""status":"SUBMITTED"""")
        }

        "return api status report - PROCESSING response" in {
            val response = route(FakeRequest(GET, "/dataset/request/read/dev-portal1/314622D8538A2BBBE77656E6B7342675")).get
            status(response) must equalTo(OK)
            contentAsString(response) must contain(""""status":"PROCESSING"""")
        }

        "return api status report - COMPLETED response" in {
            val response = route(FakeRequest(GET, "/dataset/request/read/dev-portal2/84AA0563AB9FB887AE4AE362D6B4555C")).get
            status(response) must equalTo(OK)
            contentAsString(response) must contain(""""status":"COMPLETED"""")
        }

        "return file location when status as COMPLETED in response" in {
            val response = route(FakeRequest(GET, "/dataset/request/read/dev-portal2/84AA0563AB9FB887AE4AE362D6B4555C")).get
            status(response) must equalTo(OK)
            contentAsString(response) must contain(""""location":"http://"""")
        }

        "return file size when status as COMPLETED in response" in {
            val response = route(FakeRequest(GET, "/dataset/request/read/dev-portal2/84AA0563AB9FB887AE4AE362D6B4555C")).get
            status(response) must equalTo(OK)
            contentAsString(response) must contain(""""file_size":12333""")
        }

        "return latency when status as COMPLETED in response" in {
            val response = route(FakeRequest(GET, "/dataset/request/read/dev-portal2/84AA0563AB9FB887AE4AE362D6B4555C")).get
            status(response) must equalTo(OK)
            contentAsString(response) must contain(""""latency":22345""")
        }

        "return execution time when status as COMPLETED in response" in {
            val response = route(FakeRequest(GET, "/dataset/request/read/dev-portal2/84AA0563AB9FB887AE4AE362D6B4555C")).get
            status(response) must equalTo(OK)
            contentAsString(response) must contain(""""execution_time":12345""")
        }

        "return api status report - list response" in {
            val response = route(FakeRequest(GET, "/dataset/request/list/dev-portal2?limit=50")).get
            status(response) must equalTo(OK)
            contentAsString(response) must contain(""""count":1""")
        }

    }
}