package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Dispatcher
import com.datastax.spark.connector.cql.CassandraConnector
import org.ekstep.analytics.framework.DataFilter

class TestDataExhaustModel extends SparkSpec(null) {
  
    ignore should "generate zip file of all events and save to s3" in {

        populateCassandra();
        val rdd = loadFile[Event]("src/test/resources/data-exhaust/test_data1.log");
        val rdd2 = DataExhaustModel.execute(rdd, Option(Map("request_id" -> "6a54bfa283de43a89086e69e2efdc9eb6750493d", "tags" -> List("6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"), "local_path" -> "src/test/resources/data-exhaust/", "key" -> "data-exhaust/test/")));
        
        val table = sc.cassandraTable[JobSummary]("general_db", "jobs").where("request_id=?", "6a54bfa283de43a89086e69e2efdc9eb6750493d").first
        table.output_events.get should be(100)
        table.dt_first_event.get.getMillis should be(1477980487115L)
        table.dt_last_event.get.getMillis should be(1479619192051L)
    }
    
    ignore should "generate zip file of only assessment events and save to s3" in {

        populateCassandra();
        val events = loadFile[Event]("src/test/resources/data-exhaust/test_data2.log");
        val rdd = DataFilter.filter(events, Filter("eventId", "IN", Option(List("OE_ASSESS", "OE_LEVEL_SET"))));
        val rdd2 = DataExhaustModel.execute(rdd, Option(Map("request_id" -> "4a54bfa283de43a89086e69e2efdc9eb6750493d", "tags" -> List("6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"), "local_path" -> "src/test/resources/data-exhaust/", "key" -> "data-exhaust/test/")));
        
        val table = sc.cassandraTable[JobSummary]("general_db", "jobs").where("request_id=?", "4a54bfa283de43a89086e69e2efdc9eb6750493d").first
        table.output_events.get should be(50)
        table.dt_first_event.get.getMillis should be(1477987602000L)
        table.dt_last_event.get.getMillis should be(1479629266000L)
    }
    
    ignore should "check for given filter criteria that no data available" in {

        populateCassandra();
        val rdd = loadFile[Event]("src/test/resources/data-exhaust/test_data3.log");
        val rdd2 = DataExhaustModel.execute(rdd, Option(Map("request_id" -> "8a54bfa283de43a89086e69e2efdc9eb6750493d", "tags" -> List("6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"), "local_path" -> "src/test/resources/data-exhaust/", "key" -> "data-exhaust/test/")));
        
        val table = sc.cassandraTable[JobSummary]("general_db", "jobs").where("request_id=?", "8a54bfa283de43a89086e69e2efdc9eb6750493d").first
        table.output_events.get should be(0)
//        table.locations.size should be(0)
    }
    
    ignore should "Check for duplicate events if 2 tags are selected" in {

        populateCassandra();
        val rdd = loadFile[Event]("src/test/resources/data-exhaust/test_data4.log");
        val rdd2 = DataExhaustModel.execute(rdd, Option(Map("request_id" -> "2a54bfa283de43a89086e69e2efdc9eb6750493d", "tags" -> List("6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5", "8da8fa317798fd23e6d30cdb3b7aef10c7e7bef4"), "local_path" -> "src/test/resources/data-exhaust/", "key" -> "data-exhaust/test/")));
        
        val table = sc.cassandraTable[JobSummary]("general_db", "jobs").where("request_id=?", "2a54bfa283de43a89086e69e2efdc9eb6750493d").first
        table.output_events.get should be(25)
        table.dt_first_event.get.getMillis should be(1477980487115L)
        table.dt_last_event.get.getMillis should be(1478067027149L)
    }
    
    def populateCassandra() {
        
//        CassandraConnector(sc.getConf).withSessionDo { session =>
//            session.execute("TRUNCATE general_db.jobs;");
//        }
        val input = sc.parallelize(Array(("6a54bfa283de43a89086e69e2efdc9eb6750493d", "dataexhaust", "SUBMITTED"),("4a54bfa283de43a89086e69e2efdc9eb6750493d", "dataexhaust", "SUBMITTED"),("8a54bfa283de43a89086e69e2efdc9eb6750493d", "dataexhaust", "SUBMITTED"),("2a54bfa283de43a89086e69e2efdc9eb6750493d", "dataexhaust", "SUBMITTED")));
        input.saveToCassandra("general_db", "jobs", SomeColumns("request_id","job_id","status"))

    }
}