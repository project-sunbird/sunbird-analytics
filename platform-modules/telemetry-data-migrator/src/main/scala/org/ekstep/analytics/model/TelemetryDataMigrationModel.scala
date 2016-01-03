package org.ekstep.analytics.model

import org.apache.spark.rdd.RDD
import org.ekstep.analytics.LineData
import org.ekstep.analytics.SyncEvent
import org.ekstep.analytics.framework.util.JSONUtils

object TelemetryDataMigrationModel extends Serializable {

    def execute(rdd: RDD[SyncEvent]): RDD[String] = {
        rdd.filter(_ != null)
            .map { x =>
                {
                    val ld = x.data.getOrElse(LineData(None, None, None, None, Array()));
                    val ts = ld.ts;
                    if (ld.events != null) {
                        ld.events.map(f => {
                            val m = scala.collection.mutable.Map(f.toSeq: _*);
                            m += "@timestamp" -> ts.getOrElse(m.get("ts")).asInstanceOf[AnyRef];
                        });
                    } else {
                        null;
                    }
                }
            }
            .filter { x => x != null }
            .flatMap(f => f.map(JSONUtils.serialize(_)));
    }

}