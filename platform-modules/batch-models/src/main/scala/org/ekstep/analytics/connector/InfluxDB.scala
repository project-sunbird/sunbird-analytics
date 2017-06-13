/**
 * @author Mahesh Kumar Gangula
 */

package org.ekstep.analytics.connector

import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework.dispatcher.InfluxDBDispatcher._
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.commons.lang3.StringUtils
import org.apache.spark.SparkContext
import com.datastax.spark.connector._
import org.ekstep.analytics.util.Constants
import org.ekstep.analytics.updater.UserProfile
import org.ekstep.analytics.updater.AppObjectCache
import org.ekstep.analytics.framework.Output
import org.ekstep.analytics.framework.AlgoOutput

/**
 * Case Classes to fetch cache data from cassandra.
 */

case class UserProfileIndex(user_id: String) extends Output with AlgoOutput;
case class AppObjectCacheIndex(`type`: String, id: String) extends Output with AlgoOutput;

/**
 * Extending Spark API to save to Influx DB and adding denormalized data.
 */

class InfluxDB(rdd: RDD[InfluxRecord]) {

	def saveToInflux(measurement: String) = dispatch(measurement, rdd);
	def denormalize(mappingKey: String, newKey: String, data: Map[String, AnyRef] = Map()) = new DenormInfluxData(rdd, mappingKey, newKey, data);

}

object InfluxDB {
	implicit def methods(rdd: RDD[InfluxRecord]) = new InfluxDB(rdd: RDD[InfluxRecord]);
	def getDenormalizedData(`type`: String, data: RDD[String])(implicit sc: SparkContext): Map[String, AnyRef] = {
		`type` match {
			case "User" =>
				val users = data.map { x => UserProfileIndex(x) }.joinWithCassandraTable[UserProfile](Constants.CREATION_KEY_SPACE_NAME, Constants.USER_PROFILE_TABLE).on(SomeColumns("user_id"))
				users.map(f => (f._1.user_id, f._2.name)).collectAsMap().toMap;
			case _ =>
				val cacheObjects = data.map { x => AppObjectCacheIndex(`type`, x) }.joinWithCassandraTable[AppObjectCache](Constants.CREATION_KEY_SPACE_NAME, Constants.APP_OBJECT_CACHE_TABLE).on(SomeColumns("type", "id"));
				cacheObjects.map(f => (f._1.id, f._2.name)).collectAsMap().toMap;
		}
	}
}

class DenormInfluxData(rdd: RDD[InfluxRecord], mappingKey: String, newKey: String, data: Map[String, AnyRef]) extends RDD[InfluxRecord](rdd) with Serializable {

	override def compute(split: Partition, context: TaskContext): Iterator[InfluxRecord] = {
		firstParent[InfluxRecord].iterator(split, context)
			.map { x =>
				val key = x.tags.getOrElse(mappingKey, "");
				if(StringUtils.isBlank(key)) x else {
					val value = data.getOrElse(key, key);
					val fields = x.fields ++ Map(newKey -> value);
					InfluxRecord(x.tags, fields, x.time);	
				}
			};
	}

	override protected def getPartitions: Array[Partition] = firstParent[InfluxRecord].partitions;
}