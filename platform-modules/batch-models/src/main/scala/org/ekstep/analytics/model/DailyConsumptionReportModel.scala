package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.Level.{ERROR, INFO}
import org.ekstep.analytics.framework.dispatcher.ScriptDispatcher
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.{Empty, IBatchModelTemplate}
import org.joda.time.format.DateTimeFormat

object DailyConsumptionReportModel extends IBatchModelTemplate[Empty, Empty, Empty, Empty] {

  implicit val className: String = "org.ekstep.analytics.model.AdhocConsumptionReportModel"
  override def name: String = "DailyConsumptionReportModel"

  /**
    * This data product does not have pre-processing required. The data products reads the raw telemetry and summary events
    */
  override def preProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {
    data
  }

  /**
    * The daily consumption metrics and
    */
  override def algorithm(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {

    val scriptDirectory = config.getOrElse("adhoc_scripts_dir", "/home/analytics/telemetryreports/scripts")
    val scriptOutputDirectory = config.getOrElse("adhoc_scripts_output_dir", "/mount/data/store")
    val virtualEnvDirectory = config.getOrElse("adhoc_scripts_virtualenv_dir", "/mount/venv")
    val druidBrokerUrl = config.getOrElse("druid_broker_url", "http://localhost:8082/")
    val formatter = DateTimeFormat.forPattern("yyyy-MM-dd")
    val executionDate = config.get("startDate").map {
      d => formatter.parseDateTime(d.asInstanceOf[String]).plusDays(1).toString("dd/MM/yyyy")
    }
    val wfsDir = config.get("derived_summary_dir")

    JobLogger.log("Running daily metrics job", None, INFO)

    val dailyConsumptionReportScript =
      Seq("bash", "-c",
        s"source $virtualEnvDirectory/bin/activate; " +
        s"python $scriptDirectory/daily_metrics_refactored.py $scriptOutputDirectory $druidBrokerUrl ${ executionDate.map { dt => s"-execution_date $dt" }.getOrElse("") } ${ wfsDir.map(dir => s"-derived_summary_dir $dir").getOrElse("") }")
    println("Consumption reports command: " + dailyConsumptionReportScript)
    val dailyReportsExitCode = ScriptDispatcher.dispatch(dailyConsumptionReportScript)

    if (dailyReportsExitCode == 0) {
      JobLogger.log(s"Daily metrics job completed with exit code $dailyReportsExitCode", None, INFO)

      JobLogger.log("Running Landing page report", None, INFO)
      val landingPageScript = Seq("bash", "-c", s"source $virtualEnvDirectory/bin/activate; python $scriptDirectory/landing_page.py $scriptOutputDirectory")
      val landingPageReportExitCode = ScriptDispatcher.dispatch(landingPageScript)

      if (landingPageReportExitCode == 0) {
        JobLogger.log(s"Landing page report completed with exit code $landingPageReportExitCode", None, INFO)
      } else {
        JobLogger.log(s"Landing page report failed with exit code $landingPageReportExitCode", None, ERROR)
        throw new Exception(s"Landing page report failed with exit code $landingPageReportExitCode")
      }

    } else {
      JobLogger.log(s"Daily metrics job failed with exit code $dailyReportsExitCode", None, ERROR)
      throw new Exception(s"Daily metrics job failed with exit code $dailyReportsExitCode")
    }

    sc.emptyRDD
  }

  /**
    * Post processing on the algorithm output. Some of the post processing steps are
    * 1. Saving data to Cassandra
    * 2. Converting to "MeasuredEvent" to be able to dispatch to Kafka or any output dispatcher
    * 3. Transform into a structure that can be input to another data product
    */
  override def postProcess(events: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {
    events
  }
}
