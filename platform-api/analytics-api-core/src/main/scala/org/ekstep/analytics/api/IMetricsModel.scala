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
import org.ekstep.analytics.framework.conf.AppConf

/**
 * @author mahesh
 */

class BaseMetric(val d_period: Option[Int] = None) extends AnyRef with Serializable
trait Metrics extends BaseMetric with Serializable
trait IMetricsModel[T <: Metrics, R <: Metrics] {

    val periodMap = Map[String, (Period, Int)]("LAST_7_DAYS" -> (DAY, 7), "LAST_5_WEEKS" -> (WEEK, 5), "LAST_12_MONTHS" -> (MONTH, 12), "CUMULATIVE" -> (CUMULATIVE, 0));

    /**
     * Name of the metric.
     */
    def metric(): String = "metricName";

    /**
     * Pre-process before fetching metrics and summary.
     * 1. Validate and refresh Content List cache.
     */
    def preProcess()(implicit sc: SparkContext, config: Config) = {}

    /**
     * Wrapper for the actual fetch implementation.
     * Here we are computing actual time taken to fetch data.
     */
    def fetch(contentId: String, tag: String, period: String, fields: Array[String] = Array(), channel: String)(implicit sc: SparkContext, config: Config, mf: Manifest[T]): Map[String, AnyRef] = {
        preProcess();
        val tags = tag.split(",").distinct
        val timeTaken = org.ekstep.analytics.framework.util.CommonUtil.time({
            _fetch(contentId, tags, period, fields, channel);
        });
        timeTaken._2;
    }

    /**
     * Generic method of fetch metrics.
     * 1. Fetch data from S3 based on request (type, tags, content_id, period)
     * 2. Do tag aggregation (only for ContentUsage, GenieLaunch)
     * 3. Compute metrics by period and add empty records for the periods not available from S3 fetch.
     * 4. Compute summary by taking period metrics as input.
     * 5. Return Map as a result.
     */
    private def _fetch(contentId: String, tags: Array[String], period: String, fields: Array[String] = Array(), channel: String)(implicit sc: SparkContext, config: Config, mf: Manifest[T]): Map[String, AnyRef] = {
        try {
            val dataFetch = org.ekstep.analytics.framework.util.CommonUtil.time({
                val records = getData[T](contentId, tags, period.replace("LAST_", "").replace("_", ""), channel).cache();
                records;
            });

            val aggregated = if ("ius".equals(metric())) dataFetch._2 else
                dataFetch._2.groupBy { x => x.d_period.get }.mapValues { x => x }.map(f => f._2).asInstanceOf[RDD[Iterable[Metrics]]]
                    .map { x => x.reduce((a, b) => reduce(a.asInstanceOf[R], b.asInstanceOf[R], fields)) }.asInstanceOf[RDD[T]]
            getResult(aggregated, period, fields);
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

    /**
     * Compute metrics by period and add empty records for the periods not available from S3 fetch.
     * Each metric model will override this method.
     */
    def getMetrics(records: RDD[T], period: String, fields: Array[String] = Array())(implicit sc: SparkContext, config: Config): RDD[R]

    /**
     * Get the final summary object.
     * Remove period label properties from summary.
     */
    def getSummary(summary: R): R

    /**
     * Compute summary by taking period metrics(i.e, getMetrics() output) as input.
     */
    def computeSummary(metrics: RDD[R], fields: Array[String] = Array()): R = {
        val summary = metrics.reduce((a, b) => reduce(a, b, fields));
        getSummary(summary);
    }

    /**
     * Reduce job to compute the summary or do aggregation.
     * Each metric model will override this method.
     * This is used by getSummary, Tag aggregation.
     */
    def reduce(fact1: R, fact2: R, fields: Array[String] = Array()): R

    private def getResult(records: RDD[T], period: String, fields: Array[String] = Array())(implicit sc: SparkContext, config: Config): Map[String, AnyRef] = {
        val metrics = getMetrics(records, period, fields);
        val summary = computeSummary(metrics, fields);
        Map[String, AnyRef](
            "metrics" -> metrics.collect(),
            "summary" -> summary);
    }

    /**
     * Generic method to fetch data from S3 for the metrics.
     * It is based on request (type, tags, content_id, period)
     */
    private def getData[T](contentId: String, tags: Array[String], period: String, channel: String)(implicit mf: Manifest[T], sc: SparkContext, config: Config): RDD[T] = {
        val basePath = config.getString("metrics.search.params.path");
        val filePaths = tags.map { tag =>
            if ("gls".equals(metric)) {
                s"$basePath$metric-$tag-$channel-$period.json";
            } else {
                s"$basePath$metric-$tag-$contentId-$channel-$period.json";
            }
        }
        val queriesS3 = filePaths.map { filePath => Query(Option(config.getString("metrics.search.params.bucket")), Option(filePath)) }
        val queriesLocal = filePaths.map { filePath => Query(None, None, None, None, None, None, None, None, None, Option(filePath)) }

        val search = config.getString("metrics.search.type") match {
            case "local" => Fetcher("local", None, Option(queriesLocal));
            case "s3"    => Fetcher("s3", None, Option(queriesS3));
            case _       => throw new DataFetcherException("Unknown fetcher type found");
        }
        DataFetcher.fetchBatchData[T](search);
    }

    /**
     * Get actual period with respect to current day.
     */
    protected def _getPeriods(period: String): Array[Int] = {
        val key = periodMap.get(period).get;
        org.ekstep.analytics.framework.util.CommonUtil.getPeriods(key._1, key._2);
    }
}