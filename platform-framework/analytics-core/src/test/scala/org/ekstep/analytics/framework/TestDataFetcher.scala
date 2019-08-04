package org.ekstep.analytics.framework

import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.JSONUtils
import io.circe.generic.auto._

/**
 * @author Santhosh
 */


case class GroupByPid(context_pdata_id: String, count: Int)
case class TopNData(data: List[GroupByPid])
case class TimeSeriesData(time: String, count: Int)

class TestDataFetcher extends SparkSpec {

//    implicit val decoder  = null
    "DataFetcher" should "fetch the batch events matching query" in {
        
        val queries = Option(Array(
            Query(Option("ekstep-dev-data-store"), Option("testUpload/"), Option("2016-01-01"), Option("2016-01-01"))
        ));
        val rdd = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queries));
        rdd.count should be (1701)
        
        val queries1 = Option(Array(
            Query(Option("ekstep-dev-data-store"), Option("testUpload/"), Option("2016-01-01"), Option("2016-01-01"), None, None, None, None, None, None, Option("2016-01-01-20160312.json.gz"))
        ));
        val rdd1 = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queries1));
        rdd1.count should be (0)
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
        
        val search0 = Fetcher("local", None, Option(Array(
            Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry_2.log"))
        )));
        val rddString = DataFetcher.fetchBatchData[String](search0);
        rddString.count should be (19)
        
        val search1 = Fetcher("local", None, Option(Array(
            Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))
        )));
        val rdd1 = DataFetcher.fetchBatchData[TestDataFetcher](search1);
        rdd1.count should be (0)
    }
    
    it should "fetch no file from S3 and return an empty RDD" in {
        val queries = Option(Array(
            Query(Option("ekstep-dev-data-store"), Option("abc/"), Option("2012-01-01"), Option("2012-02-01"))
        ));
        val rdd = DataFetcher.fetchBatchData[Event](Fetcher("S3", None, queries));
        rdd.isEmpty() should be (true)
    }
    
    it should "throw DataFetcherException" in {
        
        // Throw unknown fetcher type found
        the[DataFetcherException] thrownBy {
            DataFetcher.fetchBatchData[Event](Fetcher("s3", None, None));    
        }
        
        the[DataFetcherException] thrownBy {
            val fileFetcher = Fetcher("file", None, Option(Array(
                Query(None, None, None, None, None, None, None, None, None, Option("src/test/resources/sample_telemetry.log"))
            )));
            DataFetcher.fetchBatchData[Event](fileFetcher);
        } should have message "Unknown fetcher type found"
    }

    it should "fetch the batch events from azure" in {

        val queries = Option(Array(
            Query(Option("dev-data-store"), Option("raw/"), Option("2017-08-31"), Option("2017-08-31"))
        ));
        val rdd = DataFetcher.fetchBatchData[Event](Fetcher("azure", None, queries));
        println(rdd.count)
//        rdd.count should be (227)

        val queries1 = Option(Array(
            Query(Option("dev-data-store"), Option("raw/"), Option("2018-07-01"), Option("2018-07-01"))
        ));
        val rdd1 = DataFetcher.fetchBatchData[Event](Fetcher("azure", None, queries1));
        println(rdd1.count)
//        rdd1.count should be (0)
    }

    it should "fetch the data from druid" in {

        val groupByQuery = DruidQueryModel("groupBy", "telemetry-events", "LastDay", Option("all"), Option(List(Aggregation("count", "count", None))), Option(List("context_pdata_id")))
        val groupByRes = DruidDataFetcher.getDruidData[GroupByPid](groupByQuery)
        groupByRes.foreach(f => println(JSONUtils.serialize(f)))
////
        val topNQuery = DruidQueryModel("topN", "telemetry-events", "LastWeek", Option("all"), Option(List(Aggregation("count", "count", None))), Option(List("context_pdata_id")))
//        val topNRes = DruidDataFetcher.getDruidData[GroupByPid](topNQuery)
//        topNRes.foreach(f => println(f))
//
        val tsQuery = DruidQueryModel("timeSeries", "telemetry-events", "LastWeek", Option("day"), Option(List(Aggregation("count", "count", None))))
//        val tsRes = DruidDataFetcher.getDruidData[TimeSeriesData](tsQuery)
//        tsRes.foreach(f => println(f))

//        val rdd1 = DataFetcher.fetchBatchData[GroupByPid](Fetcher("druid", None, None, Option(groupByQuery)));
//        println(rdd1.count())
//        rdd1.foreach(f => println(f))
//
//        val rdd2 = DataFetcher.fetchBatchData[TimeSeriesData](Fetcher("druid", None, None, Option(tsQuery)));
//        println(rdd2.count())
//        rdd2.foreach(f => println(f))

//        val rdd3 = DataFetcher.fetchBatchData[TopNData](Fetcher("druid", None, None, Option(topNQuery)));
//        println(rdd3.count())
//        rdd3.foreach(f => println(f))
    }
}