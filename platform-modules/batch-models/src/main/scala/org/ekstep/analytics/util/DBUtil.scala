package org.ekstep.analytics.util

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.ekstep.analytics.model.ContentSideloading
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.util.CommonUtil

object DBUtil {

        def main(args: Array[String]): Unit = {
        val sc = CommonUtil.getSparkContext(8, "test")
        val records = sc.cassandraTable[ContentSideloading](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONTENT_SIDELOADING_SUMMARY)
        val rows = records.map { x => JSONUtils.serialize(x) }.collect;
        for (row <- rows) {
            println(row)
        }
    }
}