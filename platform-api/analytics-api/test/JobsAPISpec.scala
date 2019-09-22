
import org.cassandraunit.CQLDataLoader
import org.cassandraunit.dataset.cql.FileCQLDataSet
import org.cassandraunit.utils.EmbeddedCassandraServerHelper
import org.ekstep.analytics.api.JobRequest
import org.ekstep.analytics.api.util.{CacheUtil, DBUtil, PostgresDBUtil}
import org.joda.time.DateTime
import org.junit.runner._
import org.specs2.runner._
import play.api.test.Helpers._
import play.api.test._

import scala.concurrent.Await
import scala.concurrent.duration.DurationInt
@RunWith(classOf[JUnitRunner])
class JobsAPISpec extends BaseSpec with Serializable {

    val url = "/dataset/request/submit";

    def beforeAll() {
        EmbeddedCassandraServerHelper.startEmbeddedCassandra();

        val session = DBUtil.cluster.connect();
        val dataLoader = new CQLDataLoader(session);
        dataLoader.load(new FileCQLDataSet(config.getString("cassandra.cql_path"), true, true));
        session.execute("TRUNCATE platform_db.job_request");
    }

    def afterAll() {
        EmbeddedCassandraServerHelper.cleanEmbeddedCassandra()
        EmbeddedCassandraServerHelper.stopEmbeddedCassandra()
    }

    "JobRequestAPI" should {

        val headersWrong = FakeHeaders(Seq(("X-Consumer-ID", "test"), ("X-Channel-ID", "ekstep")))
        val headersRight = FakeHeaders(Seq(("X-Consumer-ID", "test_id_01"), ("X-Channel-ID", "in.ekstep")))

        "return error response on invalid request - error response" in new WithApplication() {
            val request = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal" },"request": {"filter": {"start_date": "2016-11-20","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]  }}}"""
            val response = post(url, request, headersRight);
            hasClientError(response);
            status(response) must equalTo(BAD_REQUEST)
            val response1 = post(url, request, headersWrong);
            status(response1) must equalTo(FORBIDDEN)
        }
        step("stop application")

        "return the result - successful response" in new WithApplication() {
            val request = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal" },"request": {"filter": {"start_date": "2016-11-03","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"]  }}}"""
            val response = post(url, request, headersRight);
            isOK(response);
            status(response) must equalTo(OK)
        }

        step("stop application")

        "return error response on empty request - error response" in new WithApplication() {
            val request = """ {}"""
            val response = post(url, request, headersRight);
            hasClientError(response);
            status(response) must equalTo(BAD_REQUEST)
        }

        step("stop application")

        "return status as processed when submitted job is processed" in new WithApplication() {
            val request = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal1" },"request": {"filter": {"start_date": "2016-11-04","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef7"]  }}}"""
            val response = post(url, request, headersRight);
            val rowRDD = DBUtil.getJobRequestList("dev-portal1") //Context.sc.cassandraTable[JobRequest]("platform_db", "job_request");
            val filterRDD = rowRDD.filter { x => (x.client_key.get == "dev-portal1") }
            val rddJobRequest = filterRDD.map { x => JobRequest(x.client_key, x.request_id, x.job_id, Some("PROCESSING"), x.request_data, x.iteration, x.dt_job_submitted, x.location, x.dt_file_created, x.dt_first_event, x.dt_last_event, x.dt_expiration, x.dt_job_processing, x.dt_job_completed, x.input_events, x.output_events, x.file_size, x.latency, x.execution_time, x.err_message, x.stage, x.stage_status) }
            Await.result(response, 300 second)
            DBUtil.saveJobRequest(rddJobRequest)

            val request1 = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal1" },"request": {"filter": {"start_date": "2016-11-04","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef7"]  }}}"""
            val response1 = post(url, request1, headersRight);
            Await.result(response1, 300.seconds);
            contentAsString(response1) must contain(""""status":"PROCESSING"""")
        }

        step("stop application")

        "return status as completed when submitted job is completed" in new WithApplication() {
            val request = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal2" },"request": {"filter": {"start_date": "2016-11-05","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef8"]  }}}"""
            val response = post(url, request, headersRight);
            Await.result(response, 300.seconds);
            val rowRDD = DBUtil.getJobRequestList("dev-portal2")
            val filterRDD = rowRDD.filter { x => (x.client_key.get == "dev-portal2") }
            val rddJobRequest = filterRDD.map { x => JobRequest(x.client_key, x.request_id, x.job_id, Some("COMPLETED"), x.request_data, x.iteration, x.dt_job_submitted, Some("http://"), Some(new org.joda.time.DateTime(DateTime.now())), Some(new org.joda.time.DateTime(DateTime.now())), Some(new org.joda.time.DateTime(DateTime.now())), Some(new org.joda.time.DateTime(DateTime.now().plusDays(2))), Some(new org.joda.time.DateTime(DateTime.now())), Some(new org.joda.time.DateTime(DateTime.now())), Some(1000), Some(2000), Some(12333), Some(22345), Some(12345l), x.err_message, x.stage, x.stage_status) }

            DBUtil.saveJobRequest(rddJobRequest)

            val request1 = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal2" },"request": {"filter": {"start_date": "2016-11-05","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef8"]  }}}"""
            val response1 = post(url, request1, headersRight);

            contentAsString(response1) must contain(""""status":"COMPLETED"""")
            status(response) must equalTo(OK)
        }
        step("stop application")

        "return status as failed when submitted job is failed" in new WithApplication() {
            val request = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal4" },"request": {"filter": {"start_date": "2016-11-05","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef10"]  }}}"""
            val response = post(url, request1, headersRight);
            Await.result(response, 300.seconds);

            val rowRDD = DBUtil.getJobRequestList("dev-portal4")
            val filterRDD = rowRDD.filter { x => (x.client_key.get == "dev-portal4") }
            val rddJobRequest = filterRDD.map { x => JobRequest(x.client_key, x.request_id, x.job_id, Some("FAILED"), x.request_data, x.iteration, x.dt_job_submitted, Some("http://"), Some(new org.joda.time.DateTime("2016-12-28")), Some(new org.joda.time.DateTime("2016-12-28")), Some(new org.joda.time.DateTime("2016-12-28")), Some(new org.joda.time.DateTime("2016-12-28")), Some(new org.joda.time.DateTime("2016-12-28")), Some(new org.joda.time.DateTime("2016-12-28")), Some(1000), Some(2000), Some(12333), Some(22345), Some(12345l), x.err_message, x.stage, x.stage_status) }

            DBUtil.saveJobRequest(rddJobRequest)

            val request1 = """ {"id": "ekstep.analytics.data.out","ver": "1.0", "ts": "2016-12-07T12:40:40+05:30","params": { "msgid": "4f04da60-1e24-4d31-aa7b-1daf91c46341", "client_key": "dev-portal4" },"request": {"filter": {"start_date": "2016-11-05","end_date": "2016-11-10","tags": ["6da8fa317798fd23e6d30cdb3b7aef10c7e7bef10"]  }}}"""
            val response1 = post(url, request1, headersRight);

            Await.result(response1, 300.seconds);
            contentAsString(response1) must contain(""""status":"FAILED"""")
            status(response) must equalTo(OK)
        }

        step("stop application")

//        "return api status report - SUBMITTED response" in new WithApplication() {
//            val response = route(FakeRequest(GET, "/dataset/request/read/dev-portal/BA215FC2FA3A9E69210C335BCA7B2EE7")).get
//            status(response) must equalTo(FORBIDDEN)
//
//            val response1 = route(FakeRequest(GET, "/dataset/request/read/dev-portal/BA215FC2FA3A9E69210C335BCA7B2EE7").withHeaders(("X-Consumer-ID", "test_id_01"), ("X-Channel-ID", "in.ekstep"))).get
//            status(response1) must equalTo(OK)
//            contentAsString(response1) must contain(""""status":"SUBMITTED"""")
//        }
//        step("stop application")

//        "return api status report - PROCESSING response" in new WithApplication() {
//            val response = route(FakeRequest(GET, "/dataset/request/read/dev-portal1/314622D8538A2BBBE77656E6B7342675").withHeaders(("X-Consumer-ID", "test_id_01"), ("X-Channel-ID", "in.ekstep"))).get
//            status(response) must equalTo(OK)
//            contentAsString(response) must contain(""""status":"PROCESSING"""")
//        }
//
//        step("stop application")
//        "return api status report - COMPLETED response" in new WithApplication() {
//            val response = route(FakeRequest(GET, "/dataset/request/read/dev-portal2/84AA0563AB9FB887AE4AE362D6B4555C").withHeaders(("X-Consumer-ID", "test_id_01"), ("X-Channel-ID", "in.ekstep"))).get
//            status(response) must equalTo(OK)
//            contentAsString(response) must contain(""""status":"COMPLETED"""")
//        }
//
//        step("stop application")
//
//        "return file location when status as COMPLETED in response" in new WithApplication() {
//            val response = route(FakeRequest(GET, "/dataset/request/read/dev-portal2/84AA0563AB9FB887AE4AE362D6B4555C").withHeaders(("X-Consumer-ID", "test_id_01"), ("X-Channel-ID", "in.ekstep"))).get
//            status(response) must equalTo(OK)
//            contentAsString(response) must contain(""""location":"http://"""")
//        }
//
//        step("stop application")
//
//        "return file size when status as COMPLETED in response" in new WithApplication() {
//            val response = route(FakeRequest(GET, "/dataset/request/read/dev-portal2/84AA0563AB9FB887AE4AE362D6B4555C").withHeaders(("X-Consumer-ID", "test_id_01"), ("X-Channel-ID", "in.ekstep"))).get
//            status(response) must equalTo(OK)
//            contentAsString(response) must contain(""""file_size":12333""")
//        }
//        step("stop application")
//
//        "return latency when status as COMPLETED in response" in new WithApplication() {
//            val response = route(FakeRequest(GET, "/dataset/request/read/dev-portal2/84AA0563AB9FB887AE4AE362D6B4555C").withHeaders(("X-Consumer-ID", "test_id_01"), ("X-Channel-ID", "in.ekstep"))).get
//            status(response) must equalTo(OK)
//            contentAsString(response) must contain(""""latency":22345""")
//        }
//
//        step("stop application")
//
//        "return execution time when status as COMPLETED in response" in new WithApplication() {
//            val response = route(FakeRequest(GET, "/dataset/request/read/dev-portal2/84AA0563AB9FB887AE4AE362D6B4555C").withHeaders(("X-Consumer-ID", "test_id_01"), ("X-Channel-ID", "in.ekstep"))).get
//            status(response) must equalTo(OK)
//            contentAsString(response) must contain(""""execution_time":12345""")
//        }
//
//        step("stop application")

//        "return api status report - list response" in new WithApplication() {
//            val response = route(FakeRequest(GET, "/dataset/request/list/dev-portal?limit=50").withHeaders(("X-Consumer-ID", "test_id_01"), ("X-Channel-ID", "in.ekstep"))).get
//            status(response) must equalTo(OK)
//            contentAsString(response) must contain(""""count":1""")
//        }

//        step("stop application")

        "channel exhaust - with headers with an authorized request" in new WithApplication() {
            val request = FakeRequest(GET, "/dataset/get/raw?from=2017-08-24&to=2017-08-25").withHeaders(("X-Consumer-ID", "test_id_01"), ("X-Channel-ID", "in.ekstep"))
            val response = route(app, request).get
            status(response) must equalTo(OK)
        }

        step("stop application")

        "channel exhaust - request without headers" in new WithApplication() {
            val request = FakeRequest(GET, "/dataset/get/raw?from=2017-08-24&to=2017-08-25")
            val response = route(app, request).get
            status(response) must equalTo(FORBIDDEN)
        }

        step("stop application")

        "testing consumer cache" in new WithApplication() {

            val consumer = "test_id_2"
            val channel = "diksha"
            val stat = CacheUtil.getConsumerChannlTable().get(consumer, channel)
            stat should be (null)

            val time = new DateTime()
            val query = s"INSERT INTO consumer_channel_mapping (consumer_id, channel, status, created_by, created_on, updated_on) VALUES ('$consumer', '$channel', 1, 'dev-team', '$time', '$time')"
            // val url = config.getString("postgres.url")
            // val user = config.getString("postgres.user")
            // val pass = config.getString("postgres.pass")
            // val conn = PostgresDBUtil.getConn(url, user, pass)
            // PostgresDBUtil.execute(conn, query)
            // PostgresDBUtil.read(query);
            PostgresDBUtil.executeQuery(query)


            val request = FakeRequest(GET, "/refresh-cache/ConsumerChannel")
            val response = route(app, request).get
            status(response) must equalTo(OK)
            val statUpdated = CacheUtil.getConsumerChannlTable().get(consumer, channel)
            statUpdated should be (Int.box(1))

            /*
          val stmt = conn.createStatement()
            stmt.execute(s"delete from consumer_channel_mapping where consumer_id='$consumer' AND channel='$channel'")
            stmt.close
            PostgresDBUtil.closeConn(conn)
            */
            PostgresDBUtil.executeQuery(s"delete from consumer_channel_mapping where consumer_id='$consumer' AND channel='$channel'")
        }
        step("stop application")
    }
}