package org.ekstep.analytics.model

import org.ekstep.analytics.framework.Empty
import org.ekstep.analytics.vidyavaani.job._
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.conf.AppConf
import org.joda.time.DateTime
import org.ekstep.analytics.framework.dispatcher.GraphQueryDispatcher

class TestAssetSnapshotSummaryModel extends SparkGraphSpec(null) {

    "AssetSnapshotSummaryModel" should "generate Asset snapshot" in {
        val event = AssetSnapshotSummaryModel.execute(sc.makeRDD(List(Empty())), None)
        println(JSONUtils.serialize(event.take(1)))
    }
}