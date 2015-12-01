package org.ekstep.ilimi.analytics.framework.fetcher

import org.ekstep.ilimi.analytics.framework.Query
import scala.collection.mutable.Buffer
import org.ekstep.ilimi.analytics.framework.util.CommonUtil
import scala.collection.mutable.ListBuffer
import org.ekstep.ilimi.analytics.framework.exception.DataFetcherException
import org.ekstep.ilimi.analytics.framework.exception.DataFetcherException
import org.ekstep.ilimi.analytics.framework.util.S3Util
import org.joda.time.LocalDate
import java.util.Date

/**
 * @author Santhosh
 */
object S3DataFetcher {

    @throws(classOf[DataFetcherException])
    def getObjectKeys(queries: Array[Query]): Buffer[String] = {

        var paths = ListBuffer[String]();
        queries.foreach { query =>
            paths ++= S3Util.search(getBucket(query.bucket), getPrefix(query.prefix), query.startDate, query.endDate, query.delta).filterNot { x => x.isEmpty() };
        }
        paths;
    }
    
    private def getBucket(bucket: Option[String]) : String = {
        bucket.getOrElse("ekstep-telemetry");
    }
    
    private def getPrefix(prefix: Option[String]) : String = {
        prefix.getOrElse("prod.telemetry.unique-");
    }

}