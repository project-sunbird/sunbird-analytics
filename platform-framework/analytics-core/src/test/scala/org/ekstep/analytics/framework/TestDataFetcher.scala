package org.ekstep.analytics.framework

import org.ekstep.analytics.framework.exception.DataFetcherException

/**
 * @author Santhosh
 */
class TestDataFetcher extends SparkSpec {
    
    "DataFetcher" should "fetch the batch events matching query" in {
        
        val queries = Option(Array(
            Query(Option("sandbox-ekstep-telemetry"), Option("sandbox.telemetry.unique-"), Option("2015-09-12"), Option("2015-09-24"))
        ));
        val rdd = DataFetcher.fetchBatchData[Event](sc, Fetcher("S3", None, queries));
        rdd.count should be (521)
        
    }
    
    it should "fetch the streaming events matching query" in {
        
        val rdd = DataFetcher.fetchStreamData(null, null);
        rdd should be (null);
        
    }
    
    it should "throw DataFetcherException" in {
        
        val search = Fetcher("s3", None, Option(Array(
            Query(Option("ekstep-telemetry"), Option("telemetry.raw-"), Option("2015-06-17"), Option("2015-06-18"))
        )));
        
        a[DataFetcherException] should be thrownBy {
            DataFetcher.fetchBatchData[Event](sc, search);
        }
        
        a[DataFetcherException] should be thrownBy {
            DataFetcher.fetchBatchData[Event](sc, Fetcher("s3", None, None));
        }
        
        // Throw unknown fetcher type found
        a[DataFetcherException] should be thrownBy {
            DataFetcher.fetchBatchData[Event](sc, Fetcher("file", None, None));
        }
        
    }
  
}