package org.ekstep.analytics.api.util

object APIMetrics {

    implicit val className: String ="APIMetrics"

    var lastSyncTime = 0L

    var apiCalls: Int = 0
    var dbHitCount: Int = 0
    var dbSuccessCount: Int = 0
    var dbMissCount: Int = 0
    var dbErrorCount: Int = 0
    var dbSaveSuccessCount: Int = 0
    var dbSaveErrorCount: Int = 0

    def incrementAPICalls(): Unit = {
        apiCalls += 1
    }
    def incrementDBHitCount(): Unit = {
        dbHitCount += 1
    }
    def incrementDBSuccessCount(): Unit = {
        dbSuccessCount += 1
    }
    def incrementDBMissCount(): Unit = {
        dbMissCount += 1
    }
    def incrementDBErrorCount(): Unit = {
        dbErrorCount += 1
    }
    def incrementDBSaveSuccessCount(): Unit = {
        dbSaveSuccessCount += 1
    }
    def incrementDBSaveErrorCount(): Unit = {
        dbSaveErrorCount += 1
    }

    def resetCounts(): Unit ={
        apiCalls = 0
        dbHitCount = 0
        dbSuccessCount = 0
        dbMissCount = 0
        dbErrorCount = 0
        dbSaveSuccessCount = 0
        dbSaveErrorCount = 0

    }

    def writeMetrics(interval: Int, jobName: String): Boolean = {
        val currentTime = System.currentTimeMillis()
        if(lastSyncTime == 0L ) {
            lastSyncTime = currentTime
            true
        }
        else if(currentTime - lastSyncTime >= (interval*60*1000) ) {
            lastSyncTime = currentTime
            true
        }
        else false
    }
}

