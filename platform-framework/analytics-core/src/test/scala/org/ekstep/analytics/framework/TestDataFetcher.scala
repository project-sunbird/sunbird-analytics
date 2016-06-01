package org.ekstep.analytics.framework

import org.ekstep.analytics.framework.exception.DataFetcherException

/**
 * @author Santhosh
 */
class TestDataFetcher extends SparkSpec {
    
    "DataFetcher" should "fetch the batch events matching query" in {
        
        val queries = Option(Array(
            Query(Option("sandbox-data-store"), Option("raw/"), Option("2016-01-01"), Option("2016-01-01"))
        ));
        val rdd = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queries));
        rdd.count should be (1701)
        
    }
    
    it should "fetch the streaming events matching query" in {
        
        val rdd = DataFetcher.fetchStreamData(null, null);
        rdd should be (null);
        
    }
    
    it should "fetch the events from local file" in {
        val search = Fetcher("local", None, Option(Array(
            Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))
        )));
        val rdd = DataFetcher.fetchBatchData[Event](search);
        rdd.count should be (7437)
        
    }
    
    it should "throw DataFetcherException" in {
        
        val search = Fetcher("s3", None, Option(Array(
            Query(Option("ekstep-telemetry"), Option("telemetry.raw-"), Option("2015-06-17"), Option("2015-06-18"))
        )));
        
        a[DataFetcherException] should be thrownBy {
            DataFetcher.fetchBatchData[Event](search);
        }
        
        a[DataFetcherException] should be thrownBy {
            DataFetcher.fetchBatchData[Event](Fetcher("s3", None, None));
        }
        
        // Throw unknown fetcher type found
        the[DataFetcherException] thrownBy {
            val fileFetcher = Fetcher("file", None, Option(Array(
                Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))
            )));
            DataFetcher.fetchBatchData[Event](fileFetcher);
        } should have message "Unknown fetcher type found"
        
        val search2 = Fetcher("xyz", None, Option(Array(
            Query(Option("ekstep-telemetry"), Option("telemetry.raw-"), Option("2015-06-17"), Option("2015-06-18"))
        )));
        
    }
  
}