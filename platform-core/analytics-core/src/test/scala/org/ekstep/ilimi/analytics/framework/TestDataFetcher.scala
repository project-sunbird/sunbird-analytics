package org.ekstep.ilimi.analytics.framework

import org.ekstep.ilimi.analytics.framework.exception.DataFetcherException

/**
 * @author Santhosh
 */
class TestDataFetcher extends SparkSpec {
    
    "DataFetcher" should "fetch the batch events matching query" in {
        
        val queries = Option(Array(
            Query(Option("ekstep-telemetry"), Option("telemetry.raw-"), Option("2015-06-14"), Option("2015-06-16"), None, None, None, None, None, None, None, None, None, None)
        ));
        val rdd = DataFetcher.fetchBatchData(sc, Fetcher("S3", None, queries));
        rdd.count should be (57)
        
    }
    
    "DataFetcher" should "fetch the streaming events matching query" in {
        
        val rdd = DataFetcher.fetchStreamData(null, null);
        rdd should be (null);
        
    }
    
    "DataFetcher" should "throw DataFetcherException" in {
        
        val search = Fetcher("s3", None, Option(Array(
            Query(Option("ekstep-telemetry"), Option("telemetry.raw-"), Option("2015-06-17"), Option("2015-06-18"), None, None, None, None, None, None, None, None, None, None)
        )));
        a[DataFetcherException] should be thrownBy {
            DataFetcher.fetchBatchData(sc, search);
        }
        
        a[DataFetcherException] should be thrownBy {
            DataFetcher.fetchBatchData(sc, Fetcher("s3", None, None));
        }
        
    }
  
}