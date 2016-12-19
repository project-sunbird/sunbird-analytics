package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.model.SparkSpec
import org.ekstep.analytics.framework.util.JSONUtils
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.util.CommonUtil

class TestDataExhaustJob extends SparkSpec(null) {

    "DataExhaustJob" should "execute DataExhaustJob job and won't throw any Exception" in {
        
        val input = sc.parallelize(Array(("6a54bfa283de43a89086e69e2efdc9eb6750493d", "dataexhaust", "SUBMITTED")));
        input.saveToCassandra("general_db", "jobs", SomeColumns("request_id","job_id","status"))
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/data-exhaust/test_data1.log"))))), null, None, "org.ekstep.analytics.model.DataExhaustModel", Option(Map("request_id" -> "6a54bfa283de43a89086e69e2efdc9eb6750493d", "tags" -> List("6da8fa317798fd23e6d30cdb3b7aef10c7e7bef5"), "local_path" -> "src/test/resources/data-exhaust/", "key" -> "data-exhaust/test/")), Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestDataExhaustJob"))
        DataExhaustJob.main(JSONUtils.serialize(config))(Option(sc));
        CommonUtil.deleteFile("src/test/resources/data-exhaust/6a54bfa283de43a89086e69e2efdc9eb6750493d.zip")
    }
}
