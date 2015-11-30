package org.ekstep.ilimi.analytics

import org.ekstep.ilimi.analytics.framework.JobConfig
import org.ekstep.ilimi.analytics.framework.Fetcher
import java.nio.file.DirectoryStream.Filter
import org.ekstep.ilimi.analytics.framework.Query
import org.ekstep.ilimi.analytics.framework.Dispatcher
import org.ekstep.ilimi.analytics.framework.util.JSONUtils

/**
 * @author Santhosh
 */
object TestTelemetryMigrator {

    def main(args: Array[String]): Unit = {

        val jobConfig = JobConfig(
            Fetcher("s3", None, Option(Array(Query(Option("ep-production-backup"), Option("logs/telemetry/telemetry.log-"), Option("2015-06-20"), Option("2015-11-21"))))),
            None,
            None,
            "org.ekstep.analytics.model.TelemetryDataMigrationModel",
            None,
            Option(
                Array(
                    Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])),
                    Dispatcher("kafka", Map("brokerList" -> "10.10.1.146:9092", "topic" -> "prod.telemetry.unique"))
                    )
            ),
            Option(8),
            Option("Telemetry Data Migrator"));
        Console.println("Config", JSONUtils.serialize(jobConfig));
        //TelemetryDataMigrator.main(JSONUtils.serialize(jobConfig));
    }
}