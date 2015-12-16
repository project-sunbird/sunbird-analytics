package org.ekstep.ilimi.analytics.model

import org.apache.spark.rdd.RDD
import org.ekstep.ilimi.analytics.LineData
import org.ekstep.ilimi.analytics.SyncEvent
import org.ekstep.ilimi.analytics.framework.util.JSONUtils

object TelemetryDataMigrationModel extends Serializable {

    def execute(rdd: RDD[SyncEvent]) : RDD[String] = {
        rdd.filter(_ != null)
        .map { x => {
            val ld = x.data.getOrElse(LineData(None, None, None, None, Array()));
            val ts = ld.ts;
            ld.events.map(f => {
               val m = scala.collection.mutable.Map(f.toSeq: _*);
               m += "@timestamp" -> ts.getOrElse(m.get("ts")).asInstanceOf[AnyRef];
            }); 
        }}
        .filter { x => x != null }
        .flatMap(f => f.map(JSONUtils.serialize(_)));
    }

}