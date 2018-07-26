package org.ekstep.analytics.framework.fetcher

import org.ekstep.analytics.framework.Query
//import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.sunbird.cloud.storage.factory.StorageConfig
import org.sunbird.cloud.storage.factory.StorageServiceFactory
import org.sunbird.cloud.storage.conf.AppConf

object AzureDataFetcher {

    @throws(classOf[DataFetcherException])
    def getObjectKeys(queries: Array[Query]): Array[String] = {

        val keys = for(query <- queries) yield {
            val paths = if(query.folder.isDefined && query.endDate.isDefined && query.folder.getOrElse("false").equals("true")) {
                Array("wasb://"+getBucket(query.bucket) + "@" + AppConf.getStorageKey("azure") + ".blob.core.windows.net" + "/" + getPrefix(query.prefix) + query.endDate.get)
            } else {
                getKeys(query);
            }
            if(query.excludePrefix.isDefined) {
                paths.filter { x => !x.contains(query.excludePrefix.get) }
            } else {
                paths
            }
        }
        keys.flatMap { x => x.map { x => x } }
    }

    private def getKeys(query: Query) : Array[String] = {
        StorageServiceFactory.getStorageService(StorageConfig("azure", AppConf.getStorageKey("azure"), AppConf.getStorageSecret("azure"))).searchObjectkeys(getBucket(query.bucket), getPrefix(query.prefix), query.startDate, query.endDate, query.delta, query.datePattern.getOrElse("yyyy-MM-dd")).filterNot { x => x.isEmpty() }.toArray
    }

    private def getBucket(bucket: Option[String]) : String = {
        bucket.getOrElse("prod-data-store");
    }

    private def getPrefix(prefix: Option[String]) : String = {
        prefix.getOrElse("raw/");
    }
}