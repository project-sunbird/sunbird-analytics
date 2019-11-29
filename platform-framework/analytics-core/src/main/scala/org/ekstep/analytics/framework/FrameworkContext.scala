package org.ekstep.analytics.framework

import org.sunbird.cloud.storage.factory.StorageServiceFactory
import org.sunbird.cloud.storage.factory.StorageConfig
import org.sunbird.cloud.storage.conf.AppConf
import org.sunbird.cloud.storage.BaseStorageService
import ing.wbaa.druid.DruidQuery
import ing.wbaa.druid.DruidResponse
import scala.concurrent.Future

class FrameworkContext {

  def getStorageService(storageType: String) : BaseStorageService = {
    StorageServiceFactory.getStorageService(StorageConfig(storageType, AppConf.getStorageKey(storageType), AppConf.getStorageSecret(storageType)))
  }
  
}