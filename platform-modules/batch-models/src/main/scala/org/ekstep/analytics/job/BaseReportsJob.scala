package org.ekstep.analytics.job

import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.JobContext
import org.sunbird.cloud.storage.BaseStorageService
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}

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
    val reportsStorageAccountKey = AppConf.getConfig("reports_azure_storage_key")
    val reportsStorageAccountSecret = AppConf.getConfig("reports_azure_storage_secret")
    if (reportsStorageAccountKey != null && !reportsStorageAccountSecret.isEmpty) {
      sc.hadoopConfiguration.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
      sc.hadoopConfiguration.set("fs.azure.account.key." + reportsStorageAccountKey + ".blob.core.windows.net", reportsStorageAccountSecret)
    }
  }

  def reportStorageService: BaseStorageService = {
    val reportsStorageAccountKey = AppConf.getConfig("reports_azure_storage_key")
    val reportsStorageAccountSecret = AppConf.getConfig("reports_azure_storage_secret")
    val provider = AppConf.getConfig("cloud_storage_type")
    if (reportsStorageAccountKey != null && !reportsStorageAccountSecret.isEmpty) {
      StorageServiceFactory
        .getStorageService(StorageConfig(provider, AppConf.getConfig("reports_azure_storage_key"), AppConf.getConfig("reports_azure_storage_secret")))
    } else {
      StorageServiceFactory
        .getStorageService(StorageConfig(provider, AppConf.getConfig("azure_storage_key"), AppConf.getConfig("azure_storage_secret")))
    }
  }

}