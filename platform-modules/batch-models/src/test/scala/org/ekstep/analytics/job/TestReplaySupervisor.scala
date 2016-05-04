package org.ekstep.analytics.job

import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Dispatcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.BaseSpec
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework.exception.DataFetcherException

class TestReplaySupervisor extends BaseSpec {

    ignore should " Run LP from S3 and dispatching to kafka " in {
        val config = JobConfig(Fetcher("s3", None, Option(Array(Query(Option("prod-data-store"), Option("ss/"), None, Option("__endDate__"), Option(0))))), Option(Array(Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")))), None, "org.ekstep.analytics.model.ProficiencyUpdater", Option(Map("alpha" -> 1.0d.asInstanceOf[AnyRef], "beta" -> 1.0d.asInstanceOf[AnyRef])), Option(Array(Dispatcher("console", Map("printEvent" -> Option(false))), Dispatcher("kafka", Map("brokerList" -> "localhost:9092", "topic" -> "replay")))), Option(10), Option("TestReplaySupervisor"), Option(false))
        ReplaySupervisor.main("lp", "2016-02-15", "2016-02-16", JSONUtils.serialize(config));
    }

    ignore should " Run LAS from s3 data and dispatching to kafka " in {
        val config = JobConfig(Fetcher("s3", None, Option(Array(Query(Option("prod-data-store"), Option("ss/"), None, Option("__endDate__"), Option(6))))), Option(Array(Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")))), None, "org.ekstep.analytics.model.LearnerActivitySummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> Option(false))), Dispatcher("kafka", Map("brokerList" -> "localhost:9092", "topic" -> "replay")))), Option(10), Option("TestReplaySupervisor"), Option(false))
        ReplaySupervisor.main("las", "2016-02-15", "2016-02-16", JSONUtils.serialize(config));
    }

    ignore should " Run LCAS from s3 data and dispatching to kafka " in {
        val config = JobConfig(Fetcher("s3", None, Option(Array(Query(Option("prod-data-store"), Option("ss/"), None, Option("__endDate__"), Option(6))))), Option(Array(Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")))), None, "org.ekstep.analytics.model.LearnerContentActivitySummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> Option(false))), Dispatcher("kafka", Map("brokerList" -> "localhost:9092", "topic" -> "replay")))), Option(10), Option("TestReplaySupervisor"), Option(false))
        ReplaySupervisor.main("lcas", "2016-02-01", "2016-02-02", JSONUtils.serialize(config));
    }

    ignore should " Run RE from S3 and dispatching to kafka " in {
        val config = JobConfig(Fetcher("s3", None, Option(Array(Query(Option("prod-data-store"), Option("ss/"), None, Option("__endDate__"), Option(0))))), Option(Array(Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")))), None, "org.ekstep.analytics.model.RecommendationEngine", None, Option(Array(Dispatcher("console", Map("printEvent" -> Option(false))), Dispatcher("kafka", Map("brokerList" -> "localhost:9092", "topic" -> "replay")))), Option(10), Option("TestReplaySupervisor"), Option(false))
        ReplaySupervisor.main("lcr", "2016-02-15", "2016-02-16", JSONUtils.serialize(config));
    }

    it should " Run SS from local data " in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/replay-supervisor/test_data_telemetry-__endDate__*"))))), null, null, "org.ekstep.analytics.model.LearnerSessionSummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestReplaySupervisor"), Option(true))
        ReplaySupervisor.main("ss", "2016-03-10", "2016-03-11", JSONUtils.serialize(config));
    }

    it should " Run SS V2 from local data " in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/replay-supervisor/v2_telemetry-__endDate__*"))))), null, null, "org.ekstep.analytics.model.LearnerSessionSummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestReplaySupervisor"), Option(true))
        ReplaySupervisor.main("ssv2", "2016-02-24", "2016-02-24", JSONUtils.serialize(config));
    }

    it should " Run Aser Screener Summary from local data " in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/replay-supervisor/test_data_telemetry-__endDate__*"))))), null, null, "org.ekstep.analytics.model.AserScreenSummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestReplaySupervisor"), Option(true))
        ReplaySupervisor.main("as", "2016-03-10", "2016-03-11", JSONUtils.serialize(config));
    }

    it should " Run LP from local file" in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/replay-supervisor/__endDate__*"))))), None, None, "org.ekstep.analytics.model.ProficiencyUpdater", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestReplaySupervisor"), Option(false))
        ReplaySupervisor.main("lp", "2016-02-15", "2016-02-16", JSONUtils.serialize(config));
    }
    it should " Run RE from local file " in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/replay-supervisor/__endDate__*"))))), None, None, "org.ekstep.analytics.model.RecommendationEngine", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestReplaySupervisor"), Option(false))
        ReplaySupervisor.main("lcr", "2016-02-19", "2016-02-20", JSONUtils.serialize(config))
    }

    it should " Run LAS from local file " in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/replay-supervisor/__endDate__*"))))), None, None, "org.ekstep.analytics.model.LearnerActivitySummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestReplaySupervisor"), Option(false))
        ReplaySupervisor.main("las", "2016-02-19", "2016-02-20", JSONUtils.serialize(config))
    }

    it should " Run LS from local file " in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/replay-supervisor/learner-snapshot/__endDate__*"))))), None, None, "org.ekstep.analytics.updater.UpdateLearnerActivity", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestReplaySupervisor"), Option(false))
        ReplaySupervisor.main("ls", "2016-04-01", "2016-04-06", JSONUtils.serialize(config))
    }

    it should " Run LCAS from local file " in {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/replay-supervisor/__endDate__*"))))), None, None, "org.ekstep.analytics.updater.LearnerContentActivitySummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), None, Option(false))
        ReplaySupervisor.main("lcas", "2016-02-21", "2016-02-22", JSONUtils.serialize(config))
    }
    the[Exception] thrownBy {
        val config = JobConfig(Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/replay-supervisor/__endDate__*"))))), None, None, "org.ekstep.analytics.model.LearnerActivitySummary", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), None, Option(false))
        ReplaySupervisor.main("abc", "2016-02-21", "2016-02-22", JSONUtils.serialize(config))
    } should have message "Model Code is not correct"

    it should "throw DataFetcherException" in {
        a[DataFetcherException] should be thrownBy {
            val config = JobConfig(Fetcher("s3", None, Option(Array(Query(Option("prod-data-store"), Option("ss/"), None, Option("__endDate__"), Option(0))))), Option(Array(Filter("eid", "EQ", Option("ME_SESSION_SUMMARY")))), None, "org.ekstep.analytics.model.ProficiencyUpdater", Option(Map("alpha" -> 1.0d.asInstanceOf[AnyRef], "beta" -> 1.0d.asInstanceOf[AnyRef])), Option(Array(Dispatcher("console", Map("printEvent" -> Option(false))), Dispatcher("kafka", Map("brokerList" -> "localhost:9092", "topic" -> "replay")))), Option(10), Option("TestReplaySupervisor"), Option(false))
            ReplaySupervisor.main("lp", "2015-09-02", "2015-09-02", JSONUtils.serialize(config));
        }
    }

}