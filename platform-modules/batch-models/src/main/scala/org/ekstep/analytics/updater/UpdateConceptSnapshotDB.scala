package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModel
import org.ekstep.analytics.framework._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.CommonUtil._
import org.ekstep.analytics.framework.DataFilter
import org.ekstep.analytics.framework.Filter
import org.ekstep.analytics.framework.Period._
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.framework.util.JSONUtils
import org.joda.time.DateTime
import scala.concurrent.duration._
import com.pygmalios.reactiveinflux._
import com.datastax.spark.connector._
import org.ekstep.analytics.framework.conf.AppConf

case class ConceptSnapshotSummary(d_period: Int, d_concept_id: String, total_content_count: Long, total_content_count_start: Long, live_content_count: Long, live_content_count_start: Long, review_content_count: Long, review_content_count_start: Long) extends AlgoOutput with Output
case class ConceptSnapshotIndex(d_period: Int, d_concept_id: String)

object UpdateConceptSnapshotDB extends IBatchModelTemplate[DerivedEvent, DerivedEvent, ConceptSnapshotSummary, ConceptSnapshotSummary] with Serializable {
  
    val className = "org.ekstep.analytics.updater.UpdateConceptSnapshotDB"
    override def name: String = "UpdateConceptSnapshotDB"
    val CONCEPT_SNAPSHOT_METRICS = "concept_snapshot_metrics";

    override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
        DataFilter.filter(data, Filter("eid", "EQ", Option("ME_CONCEPT_SNAPSHOT_SUMMARY")));
    }

    override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ConceptSnapshotSummary] = {

        val periodsList = List(DAY, WEEK, MONTH)
        val currentData = data.map { x =>
            for (p <- periodsList) yield {
                val d_period = CommonUtil.getPeriod(x.syncts, p);
                (ConceptSnapshotIndex(d_period, x.dimensions.concept_id.get), x);
            }
        }.flatMap(f => f)
        val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[ConceptSnapshotSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.CONCEPT_SNAPSHOT_SUMMARY).on(SomeColumns("d_period", "d_concept_id"));
        val joinedData = currentData.leftOuterJoin(prvData)
        joinedData.map { f =>
            val prevSumm = f._2._2.getOrElse(null)
            val eksMap = f._2._1.edata.eks.asInstanceOf[Map[String, AnyRef]]

            val total_content_count = eksMap.get("total_content_count").get.asInstanceOf[Number].longValue()
            val live_content_count = eksMap.get("live_content_count").get.asInstanceOf[Number].longValue()
            val review_content_count = eksMap.get("review_content_count").get.asInstanceOf[Number].longValue()

            if (null == prevSumm)
                ConceptSnapshotSummary(f._1.d_period, f._1.d_concept_id, total_content_count, total_content_count, live_content_count, live_content_count, review_content_count, review_content_count)
            else
                ConceptSnapshotSummary(f._1.d_period, f._1.d_concept_id, total_content_count, prevSumm.total_content_count_start, live_content_count, prevSumm.live_content_count_start, review_content_count, prevSumm.review_content_count_start)
        }
    }

    override def postProcess(data: RDD[ConceptSnapshotSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ConceptSnapshotSummary] = {
        data.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.CONCEPT_SNAPSHOT_SUMMARY)
        saveToInfluxDB(data);
        data;
    }
    
    private def saveToInfluxDB(data: RDD[ConceptSnapshotSummary]) {
    	val metrics = data.map { x =>
			val fields: Map[com.pygmalios.reactiveinflux.Point.FieldKey, com.pygmalios.reactiveinflux.FieldValue] = Map(
					"total_content_count" -> x.total_content_count.toDouble,
					"total_content_count_start" -> x.total_content_count_start.toDouble,
					"live_content_count" -> x.live_content_count.toDouble,
					"live_content_count_start" -> x.live_content_count_start.toDouble,
					"review_content_count" -> x.review_content_count.toDouble,
					"review_content_count_start" -> x.review_content_count_start.toDouble);
        	Point(time = getDateTime(x.d_period), 
        			measurement = CONCEPT_SNAPSHOT_METRICS, 
        			tags = Map("env" -> AppConf.getConfig("application.env"), "concept_id" -> x.d_concept_id), 
        			fields = fields);
        };
        import com.pygmalios.reactiveinflux.spark._
        implicit val params = ReactiveInfluxDbName(AppConf.getConfig("reactiveinflux.database"))
        implicit val awaitAtMost = Integer.parseInt(AppConf.getConfig("reactiveinflux.awaitatmost")).second
        metrics.saveToInflux();
    }
    
    private def getDateTime(periodVal: Int): DateTime = {
    	val period = periodVal.toString();
        period.size match {
            case 8 => dayPeriod.parseDateTime(period).withTimeAtStartOfDay();
            case 7 =>
                val week = period.substring(0, 4) + "-" + period.substring(5, period.length);
                val firstDay = weekPeriodLabel.parseDateTime(week)
                val lastDay = firstDay.plusDays(6);
                lastDay.withTimeAtStartOfDay();
            case 6 => monthPeriod.parseDateTime(period).withTimeAtStartOfDay();
        }
    }
}