package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.conf.AppConf

trait BaseReportsJob {

  def getReportingSparkContext(config: JobConfig)(implicit sc: Option[SparkContext] = None): SparkContext = {

    val sparkContext = sc match {
      case Some(value) => {
        value
      }
      case None => {
        val sparkCassandraConnectionHost = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkCassandraConnectionHost")
        val sparkElasticsearchConnectionHost = config.modelParams.getOrElse(Map[String, Option[AnyRef]]()).get("sparkElasticsearchConnectionHost")
        CommonUtil.getSparkContext(JobContext.parallelization, config.appName.getOrElse(config.model), sparkCassandraConnectionHost, sparkElasticsearchConnectionHost)
      }
    }
    setReportsStorageConfiguration(sparkContext)
    sparkContext;

  }

  def setReportsStorageConfiguration(sc: SparkContext) {
    val reportsStorageAccount = AppConf.getConfig("reports_azure_storage_key")
    val reportsStorageAccountKey = AppConf.getConfig("reports_azure_storage_secret")
    if (reportsStorageAccount != null && !reportsStorageAccount.isEmpty()) {
      sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      sc.hadoopConfiguration.set("fs.azure.account.key." + reportsStorageAccount + ".blob.core.windows.net", reportsStorageAccountKey)
    }
  }

}