package org.ekstep.ilimi.analytics.model

import org.apache.spark.rdd.RDD
import org.ekstep.ilimi.analytics.LineData
import org.ekstep.ilimi.analytics.SyncEvent
import org.ekstep.ilimi.analytics.framework.util.JSONUtils

object TelemetryDataMigrationModel extends Serializable {

    def execute(rdd: RDD[SyncEvent]) : RDD[String] = {
        rdd.filter(_ != null).map { x => x.data.getOrElse(LineData(None, None, None, None, Array())).events }.filter { x => x != null }.flatMap(f => f.map(JSONUtils.serialize(_)));
    }

}