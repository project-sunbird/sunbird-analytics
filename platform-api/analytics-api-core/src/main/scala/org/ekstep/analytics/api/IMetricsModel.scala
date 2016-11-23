package org.ekstep.analytics.api

import org.ekstep.analytics.framework.Fetcher
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.ekstep.analytics.framework.Query
import org.apache.spark.SparkContext
import com.typesafe.config.Config
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.api.util.DataFetcher
import org.jets3t.service.S3ServiceException
import scala.reflect.ClassTag
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.framework.Period._

trait Metrics extends AnyRef with Serializable
trait IMetricsModel[T <: Metrics, R <: Metrics] {

    val periodMap = Map[String, (Period, Int)]("LAST_7_DAYS" -> (DAY, 7), "LAST_5_WEEKS" -> (WEEK, 5), "LAST_12_MONTHS" -> (MONTH, 12), "CUMULATIVE" -> (CUMULATIVE, 0));

    def metric(): String = "metricName";

    def preProcess()(implicit sc: SparkContext, config: Config) = {}
    
    def fetch(contentId: String, tag: String, period: String, fields: Array[String] = Array())(implicit sc: SparkContext, config: Config, mf: Manifest[T]): Map[String, AnyRef] = {
        preProcess();
    	val timeTaken = org.ekstep.analytics.framework.util.CommonUtil.time({
            _fetch(contentId, tag, period, fields);
        });
        println(s"Timetaken to fetch API data ($contentId, $tag, $period):", timeTaken._1);
        timeTaken._2;
    }

    private def _fetch(contentId: String, tag: String, period: String, fields: Array[String] = Array())(implicit sc: SparkContext, config: Config, mf: Manifest[T]): Map[String, AnyRef] = {
        try {
            val dataFetch = org.ekstep.analytics.framework.util.CommonUtil.time({
                val records = getData[T](contentId, tag, period.replace("LAST_", "").replace("_", "")).cache();
                records.collect();
                records;
            });    
            println(s"Timetaken to fetch data from S3 ($contentId, $tag, $period):", dataFetch._1);
            getResult(dataFetch._2, period, fields);
        } catch {
            case ex: S3ServiceException =>
                ex.printStackTrace();
                println("Data fetch Error(S3ServiceException):", ex.getMessage);
                getResult(sc.emptyRDD[T], period, fields);
            case ex: org.apache.hadoop.mapred.InvalidInputException =>
                println("Data fetch Error(InvalidInputException):", ex.getMessage);
                getResult(sc.emptyRDD[T], period, fields);
            case ex: Exception =>
                throw ex;
        }
    }

    def getMetrics(records: RDD[T], period: String, fields: Array[String] = Array())(implicit sc: SparkContext, config: Config): RDD[R]
    
    def getSummary(metrics: RDD[R],  fields: Array[String] = Array()): R = {
        metrics.reduce((a, b) => reduce(a, b, fields));
    }

    def reduce(fact1: R, fact2: R, fields: Array[String] = Array()): R
    
    private def getResult(records: RDD[T], period: String, fields: Array[String] = Array())(implicit sc: SparkContext, config: Config) : Map[String, AnyRef] = {
    	val metrics = getMetrics(records, period, fields);
            val summary = getSummary(metrics, fields);
            Map[String, AnyRef](
                "metrics" -> metrics.collect(),
                "summary" -> summary);
    }

    private def getData[T](contentId: String, tag: String, period: String)(implicit mf: Manifest[T], sc: SparkContext, config: Config): RDD[T] = {
        val basePath = config.getString("metrics.search.params.path");
        val filePath = if ("gls".equals(metric)) {
            s"$basePath$metric-$tag-$period.json";
        } else {
            s"$basePath$metric-$tag-$contentId-$period.json";
        }

        println("filePath:", filePath);
        val search = config.getString("metrics.search.type") match {
            case "local" => Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option(filePath)))));
            case "s3"    => Fetcher("s3", None, Option(Array(Query(Option(config.getString("metrics.search.params.bucket")), Option(filePath)))));
            case _       => throw new DataFetcherException("Unknown fetcher type found");
        }
        DataFetcher.fetchBatchData[T](search);
    }

    protected def _getPeriods(period: String): Array[Int] = {
        val key = periodMap.get(period).get;
        org.ekstep.analytics.framework.util.CommonUtil.getPeriods(key._1, key._2);
    }
}