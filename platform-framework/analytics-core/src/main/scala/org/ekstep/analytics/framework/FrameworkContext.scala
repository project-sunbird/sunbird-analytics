package org.ekstep.analytics.framework

import com.typesafe.config.ConfigFactory
import org.sunbird.cloud.storage.factory.StorageServiceFactory
import org.sunbird.cloud.storage.factory.StorageConfig
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.BaseStorageService
import ing.wbaa.druid.DruidConfig
import ing.wbaa.druid.client.{DruidClient, DruidHttpClient}

class FrameworkContext {

  var dc = DruidConfig.DefaultConfig.client

  def getStorageService(storageType: String) : BaseStorageService = {
    StorageServiceFactory.getStorageService(StorageConfig(storageType, AppConf.getStorageKey(storageType), AppConf.getStorageSecret(storageType)))
  }
  
  def getStorageService(storageType: String, storageKey: String, storageSecret: String) : BaseStorageService = {
    StorageServiceFactory.getStorageService(StorageConfig(storageType, storageKey, storageSecret))
  }

  def getDruidClient() : DruidClient = {
    if (dc != null)
      return dc;
    else {
        val config = AppConf.getConfig().getConfig("druid")
        implicit val druidConfig = DruidConfig(
            clientBackend = classOf[DruidHttpClient],
            clientConfig = config.getConfig("client-config")
        )
        DruidHttpClient.apply(druidConfig)
    }
  }

  def shutdownDruidClient() = {
    if(dc != null) {
      dc.actorSystem.terminate()
      dc.shutdown()
      dc = null;
    }
  }
  
}