package org.ekstep.ilimi.analytics

import org.codehaus.jackson.JsonParseException
import org.ekstep.ilimi.analytics.framework.DataFetcher
import org.ekstep.ilimi.analytics.framework.JobConfig
import org.ekstep.ilimi.analytics.framework.JobContext
import org.ekstep.ilimi.analytics.framework.OutputDispatcher
import org.ekstep.ilimi.analytics.framework.conf.AppConf
import org.ekstep.ilimi.analytics.framework.util.Application
import org.ekstep.ilimi.analytics.framework.util.CommonUtil
import org.ekstep.ilimi.analytics.framework.util.JSONUtils
import org.ekstep.ilimi.analytics.model.TelemetryDataMigrationModel

case class LineData(id: Option[String], ver: Option[String], ts: Option[String], params: Option[Map[String, AnyRef]], events: Array[Map[String, AnyRef]]);
case class SyncEvent(apiType: Option[String], level: Option[String], msg: Option[String], time: Option[String], errorMsg: Option[String], data: Option[LineData]);

object TelemetryDataMigrator extends Application {

    def main(config: String) {
        AppConf.init();
        val t1 = System.currentTimeMillis;
        try {
            val jobConfig = JSONUtils.deserialize[JobConfig](config);
            JobContext.parallelization = CommonUtil.getParallelization(jobConfig);
            val sc = CommonUtil.getSparkContext(JobContext.parallelization, jobConfig.appName.getOrElse(jobConfig.model));
            val rdd = DataFetcher.fetchBatchData[SyncEvent](sc, jobConfig.search);
            val output = TelemetryDataMigrationModel.execute(rdd);
            OutputDispatcher.dispatch(jobConfig.output, output);
            CommonUtil.closeSparkContext(sc);
        } catch {
            case e: JsonParseException =>
                Console.err.println("TelemetryDataMigrator:main() - JobConfig parse error", e.getClass.getName, e.getMessage);
                e.printStackTrace();
            case e: Exception =>
                Console.err.println("TelemetryDataMigrator:main() - Job error", e.getClass.getName, e.getMessage);
                e.printStackTrace();
        }
        val t2 = System.currentTimeMillis;
        Console.println("## Model run complete - Time taken to compute - " + (t2 - t1) / 1000 + " ##");
    }

}