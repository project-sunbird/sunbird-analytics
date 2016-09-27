package org.ekstep.analytics.api.service

import org.apache.spark.SparkContext
import org.ekstep.analytics.api._
import org.ekstep.analytics.framework.util.JSONUtils
import org.apache.commons.lang.StringUtils
import org.ekstep.analytics.api.exception.ClientException
import java.util.List
import java.util.Date
import org.joda.time.DateTime
import scala.util.Random
import scala.collection.JavaConversions._
import org.ekstep.analytics.framework._
import org.apache.spark.rdd.RDD
import org.apache.spark.HashPartitioner
import org.ekstep.analytics.framework.Event
import org.ekstep.analytics.api.util.CommonUtil
import org.ekstep.analytics.api.util.DataFetcher
import org.ekstep.analytics.api.Filter
import org.ekstep.analytics.framework.exception.DataFetcherException
import org.jets3t.service.S3ServiceException
import com.typesafe.config.Config
import org.apache.hadoop.mapred.InvalidInputException

/**
 * @author mahesh
 */

object MetricsAPIService {

	val periodMap = Map[String, (String, Int)]("LAST_7_DAYS" -> ("DAY", 7), "LAST_5_WEEKS" -> ("WEEK", 5), "LAST_12_MONTHS" -> ("MONTH", 12), "CUMULATIVE" -> ("CUMULATIVE",0));
	val reqPeriods = Array("LAST_7_DAYS", "LAST_5_WEEKS", "LAST_12_MONTHS", "CUMULATIVE");
	
	def contentUsage(body: MetricsRequestBody)(implicit sc: SparkContext, config: Config): String = {
		if (StringUtils.isEmpty(body.request.period) || reqPeriods.indexOf(body.request.period) == -1) {
				throw new ClientException("period is missing or invalid.");
		}
		val filter = body.request.filter.getOrElse(Filter());
		val contentId = filter.content_id.getOrElse("all");
		val tag = filter.tag.getOrElse("all");
		
		val data = fetchData[ContentUsageSummary]("cus", contentId, tag, body.request.period.replace("LAST_", "").replace("_",	""));
		val result = getResult(data, body.request.period); 
		JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.metrics.content-usage", result));
	}
	
	def fetchData[R](metric: String, contentId: String, tag: String, period: String)(implicit mf: Manifest[R], sc: SparkContext, config: Config): RDD[R] = {
		try {
			val basePath = config.getString("metrics.search.params.path");
		  	val filePath = s"$basePath$metric-$tag-$contentId-$period.json";
		  	println("filePath:", filePath);
			val search = config.getString("metrics.search.type") match {
				case "local" => Fetcher("local", None, Option(Array(Query(None, None, None, None, None, None, None, None, None, Option(filePath)))));
				case "s3" => Fetcher("s3", None, Option(Array(Query(Option(config.getString("metrics.search.params.bucket")), Option(filePath)))));
				case _ => throw new DataFetcherException("Unknown fetcher type found");
			}
			DataFetcher.fetchBatchData[R](search);
		} catch {
		  case ex @ (_: DataFetcherException | _: S3ServiceException) =>
		  		println("fetchData Error:", ex.getMessage);
		 	  	sc.emptyRDD[R];
		}
	}
	
	def getResult(records: RDD[ContentUsageSummary], period: String)(implicit sc: SparkContext, config: Config): Map[String, AnyRef] = {
		val periods = _getPeriods(period);
		val recordsRDD = records.map { x => (x.d_period, x) };
		var periodsRDD = sc.parallelize(periods.map { period => (period, ContentUsageSummary(period, None, None, None, None, None, None, None, None, None)) });
		val metrics = periodsRDD.leftOuterJoin(recordsRDD).sortBy(-_._1).map { f =>
			if(f._2._2.isDefined) f._2._2.get else f._2._1 
		}.map { x => x.label = Option(CommonUtil.getPeriodLabel(x.d_period)); x }.collect();
		val summary = getSummary(metrics);
		Map[String, AnyRef](
			"ttl" -> CommonUtil.getRemainingHours.asInstanceOf[AnyRef],
            "metrics" -> metrics,
            "summary" -> summary);
	}
	
	def getSummary(metrics: Array[ContentUsageSummary]): Map[String, AnyRef] = {
		Map()
	}
	
	private def _getPeriods(period: String): Array[Int] = {
		val key = periodMap.get(period).get;
		org.ekstep.analytics.framework.util.CommonUtil.getPeriods(key._1, key._2);
	}
}