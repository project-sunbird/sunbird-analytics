package org.ekstep.analytics.api.util

object APIMetrics {
    implicit val className: String ="APIMetrics"
    var successCount: Int = 0
    var failureCount: Int = 0
    var errorCount: Int = 0
    var lastSyncTime = 0L

    def incrementSuccessCount(): Unit = {
        successCount += 1
    }

    def incrementFailureCount(): Unit = {
        failureCount += 1
    }

    def incrementErrorCount(): Unit = {
        errorCount += 1
    }

    def resetCounts(): Unit ={
        successCount = 0
        failureCount = 0
        errorCount = 0
    }

    def updateMetrics(interval: Int, jobName: String): Unit = {
        val currentTime = System.currentTimeMillis()
        if(lastSyncTime == 0L ) {
            writeMetricsToLog(jobName);
            lastSyncTime = currentTime
        }
        else if(currentTime - lastSyncTime >= (interval*60*1000) ) {
            writeMetricsToLog(jobName);
            lastSyncTime = currentTime
        }
        else {}
    }

    def writeMetricsToLog(jobName: String) = synchronized {
        val data = Map("job-name" -> jobName, "api-version" -> "v1", "timestamp" -> System.currentTimeMillis(), "success-count" -> successCount, "failure-count" -> failureCount, "error-count" -> errorCount)
        APILogger.logMetrics(Option(data));
        // reset counts
        resetCounts()
    }

}
