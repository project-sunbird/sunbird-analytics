package org.ekstep.ilimi.analytics.framework.fetcher

import org.ekstep.ilimi.analytics.framework.Query
import scala.collection.mutable.Buffer
import org.ekstep.ilimi.analytics.framework.util.CommonUtil
import scala.collection.mutable.ListBuffer
import org.ekstep.ilimi.analytics.framework.exception.DataFetcherException
import org.ekstep.ilimi.analytics.framework.exception.DataFetcherException
import org.ekstep.ilimi.analytics.framework.util.S3Util

/**
 * @author Santhosh
 */
object S3DataFetcher {

    @throws(classOf[DataFetcherException])
    def getObjectKeys(queries: Array[Query]): Buffer[String] = {

        var paths = ListBuffer[String]();
        queries.foreach { query =>
            if (query.bucket.isEmpty) {
                throw new DataFetcherException("Bucket cannot be empty while fetching S3 object keys");
            }
            paths ++= S3Util.search(query.bucket.get, query.prefix.getOrElse(""), query.startDate, query.endDate).filterNot { x => x.isEmpty() };
        }
        paths;
    }

}