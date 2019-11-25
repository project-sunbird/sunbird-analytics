package org.ekstep.analytics.api.util

import org.ekstep.analytics.api.BaseSpec
import org.ekstep.analytics.framework.{Event, Fetcher, Query}

class TestDataFetcher extends BaseSpec {

  "Data fetcher" should "fetch data from local file" in {
      val result = DataFetcher.fetchBatchData[Event](Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/dataFetcher/test-data1.log"))))))
      result.length should be (533)
  }

  ignore should "fetch file from S3 and return data" in {
    val queries = Option(Array(
      Query(Option("dev-data-store"), Option("derived/wfs/"), Option("2019-01-03"), Option("2019-01-04"))
    ))
    val result = DataFetcher.fetchBatchData[Event](Fetcher("s3", None, queries))
    result.length should be (1000)
  }
}
