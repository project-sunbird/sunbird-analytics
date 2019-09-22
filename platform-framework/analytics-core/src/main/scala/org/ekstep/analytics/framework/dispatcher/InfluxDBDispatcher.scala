package org.ekstep.analytics.framework.dispatcher

import org.ekstep.analytics.framework.exception.DispatcherException
import org.apache.spark.rdd.RDD
import org.joda.time.DateTime

/**
 * @author mahesh
 */

object InfluxDBDispatcher {

	case class InfluxRecord(tags: Map[String, String], fields: Map[String, AnyRef], time: DateTime = DateTime.now());

	@throws(classOf[DispatcherException])
    def dispatch(measurement: String, data: RDD[InfluxRecord]) {
//		val env = Map("env" -> AppConf.getConfig("application.env"));
//		val metrics = data.map { x => Point(time = x.time, measurement= measurement, tags = Conversions.tagsToScala((x.tags ++ env).asJava), fields = Conversions.fieldsToScala(x.fields.asJava))}
//		import com.pygmalios.reactiveinflux.spark._
//		implicit val params = ReactiveInfluxDbName(AppConf.getConfig("reactiveinflux.database"))
//		implicit val awaitAtMost = Integer.parseInt(AppConf.getConfig("reactiveinflux.awaitatmost")).second
//		metrics.saveToInflux();
	}
}