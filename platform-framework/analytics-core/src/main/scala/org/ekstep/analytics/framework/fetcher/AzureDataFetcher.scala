package org.ekstep.analytics.framework.fetcher

import org.ekstep.analytics.framework.Level.INFO
import org.ekstep.analytics.framework.Query
import org.ekstep.analytics.framework.util.JobLogger
//import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.sunbird.cloud.storage.factory.{StorageConfig, StorageServiceFactory}
import org.sunbird.cloud.storage.conf.AppConf

object AzureDataFetcher {

    implicit val className = "org.ekstep.analytics.framework.fetcher.AzureDataFetcher"
    val storageService = StorageServiceFactory.getStorageService(StorageConfig("azure", AppConf.getStorageKey("azure"), AppConf.getStorageSecret("azure")))

    @throws(classOf[DataFetcherException])
    def getObjectKeys(queries: Array[Query]): Array[String] = {

        JobLogger.log("Inside getObjectKeys", Option(Map("queries" -> queries)), INFO)
        val keys = for(query <- queries) yield {
            val paths = if(query.folder.isDefined && query.endDate.isDefined && query.folder.getOrElse("false").equals("true")) {
                JobLogger.log("Inside getObjectKeys - if", None, INFO)
                Array("wasb://"+getBucket(query.bucket) + "@" + AppConf.getStorageKey("azure") + ".blob.core.windows.net" + "/" + getPrefix(query.prefix) + query.endDate.getOrElse(""))
            } else {
                JobLogger.log("Inside getObjectKeys - else", None, INFO)
                getKeys(query);
            }
            if(query.excludePrefix.isDefined) {
                paths.filter { x => !x.contains(query.excludePrefix.get) }
            } else {
                paths
            }
        }
        JobLogger.log("Inside getObjectKeys - printing keys", Option(Map("keys" -> keys)), INFO)
        keys.flatMap { x => x.map { x => x } }
    }

    private def getKeys(query: Query) : Array[String] = {
        val keys = storageService.searchObjects(getBucket(query.bucket), getPrefix(query.prefix), query.startDate, query.endDate, query.delta, query.datePattern.getOrElse("yyyy-MM-dd"))
        storageService.getPaths(getBucket(query.bucket), keys).toArray
    }

    private def getBucket(bucket: Option[String]) : String = {
        bucket.getOrElse("prod-data-store");
    }

    private def getPrefix(prefix: Option[String]) : String = {
        prefix.getOrElse("raw/");
    }
}