package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IBatchModelTemplate
import org.ekstep.analytics.framework.DerivedEvent
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.AlgoOutput
import org.ekstep.analytics.framework.Output
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.CommonUtil._
import org.ekstep.analytics.util.Constants
import com.datastax.spark.connector._
import org.joda.time.DateTime
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher.InfluxRecord
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher

case class AssetSnapshotSummary(d_period: Int, d_partner_id: String, total_images_count: Long, total_images_count_start: Long, used_images_count: Long, used_images_count_start: Long, total_audio_count: Long, total_audio_count_start: Long, used_audio_count: Long, used_audio_count_start: Long, total_questions_count: Long, total_questions_count_start: Long, used_questions_count: Long, used_questions_count_start: Long, total_activities_count: Long, total_activities_count_start: Long, used_activities_count: Long, used_activities_count_start: Long, total_templates_count: Long, total_templates_count_start: Long, used_templates_count: Long, used_templates_count_start: Long) extends AlgoOutput with Output
case class AssetSnapshotIndex(d_period: Int, d_partner_id: String)

object UpdateAssetSnapshotDB extends IBatchModelTemplate[DerivedEvent, DerivedEvent, AssetSnapshotSummary, AssetSnapshotSummary] with Serializable {

	val className = "org.ekstep.analytics.updater.UpdateAssetSnapshotDB"
	override def name: String = "UpdateAssetSnapshotDB"
	val ASSET_SNAPSHOT_METRICS = "asset_snapshot_metrics";

	override def preProcess(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DerivedEvent] = {
		data;
	}
	override def algorithm(data: RDD[DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AssetSnapshotSummary] = {

		val periodsList = List(DAY, WEEK, MONTH)
		val currentData = data.map { x =>
			for (p <- periodsList) yield {
				val d_period = CommonUtil.getPeriod(x.syncts, p);
				(AssetSnapshotIndex(d_period, x.dimensions.partner_id.get), x);
			}
		}.flatMap(f => f)

		val prvData = currentData.map { x => x._1 }.joinWithCassandraTable[AssetSnapshotSummary](Constants.CONTENT_KEY_SPACE_NAME, Constants.ASSET_SNAPSHOT_SUMMARY).on(SomeColumns("d_period", "d_partner_id"));
		val joinedData = currentData.leftOuterJoin(prvData)

		joinedData.map { f =>
			val prevSumm = f._2._2.getOrElse(null)
			val eksMap = f._2._1.edata.eks.asInstanceOf[Map[String, AnyRef]]

			val total_images_count = eksMap.get("total_images_count").get.asInstanceOf[Number].longValue
			val used_images_count = eksMap.get("used_images_count").get.asInstanceOf[Number].longValue
			val total_audio_count = eksMap.get("total_audio_count").get.asInstanceOf[Number].longValue
			val used_audio_count = eksMap.get("used_audio_count").get.asInstanceOf[Number].longValue
			val total_questions_count = eksMap.get("total_questions_count").get.asInstanceOf[Number].longValue
			val used_questions_count = eksMap.get("used_questions_count").get.asInstanceOf[Number].longValue
			val total_activities_count = eksMap.get("total_activities_count").get.asInstanceOf[Number].longValue
			val used_activities_count = eksMap.get("used_activities_count").get.asInstanceOf[Number].longValue
			val total_templates_count = eksMap.get("total_templates_count").get.asInstanceOf[Number].longValue
			val used_templates_count = eksMap.get("used_templates_count").get.asInstanceOf[Number].longValue

			if (null == prevSumm)
				AssetSnapshotSummary(f._1.d_period, f._1.d_partner_id, total_images_count, total_images_count, used_images_count, used_images_count, total_audio_count, total_audio_count, used_audio_count, used_audio_count, total_questions_count, total_questions_count, used_questions_count, used_questions_count, total_activities_count, total_activities_count, used_activities_count, used_activities_count, total_templates_count, total_templates_count, used_templates_count, used_templates_count)
			else
				AssetSnapshotSummary(f._1.d_period, f._1.d_partner_id, total_images_count, prevSumm.total_images_count_start, used_images_count, prevSumm.used_images_count_start, total_audio_count, prevSumm.total_audio_count_start, used_audio_count, prevSumm.used_audio_count_start, total_questions_count, prevSumm.total_questions_count_start, used_questions_count, prevSumm.used_questions_count_start, total_activities_count, prevSumm.total_activities_count_start, used_activities_count, prevSumm.used_activities_count_start, total_templates_count, prevSumm.total_templates_count_start, used_templates_count, prevSumm.used_templates_count_start)
		}
	}
	override def postProcess(data: RDD[AssetSnapshotSummary], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[AssetSnapshotSummary] = {
		data.saveToCassandra(Constants.CONTENT_KEY_SPACE_NAME, Constants.ASSET_SNAPSHOT_SUMMARY);
		saveToInfluxDB(data);
		data;
	}

	private def saveToInfluxDB(data: RDD[AssetSnapshotSummary]) {
		val metrics = data.map { x =>
			val fields = CommonUtil.caseClassToMap(x) - ("d_period" ,"d_partner_id")
			val time = getDateTime(x.d_period);
			InfluxRecord(Map("period" -> time._2, "partner_id" -> x.d_partner_id), fields, time._1);
		};
		InfluxDBDispatcher.dispatch(ASSET_SNAPSHOT_METRICS, metrics);
	}

	private def getDateTime(periodVal: Int): (DateTime, String) = {
		val period = periodVal.toString();
		period.size match {
			case 8 => (dayPeriod.parseDateTime(period).withTimeAtStartOfDay(), "day");
			case 7 =>
				val week = period.substring(0, 4) + "-" + period.substring(5, period.length);
				val firstDay = weekPeriodLabel.parseDateTime(week)
				val lastDay = firstDay.plusDays(6);
				(lastDay.withTimeAtStartOfDay(), "week");
			case 6 => (monthPeriod.parseDateTime(period).withTimeAtStartOfDay(), "month");
		}
	}
}