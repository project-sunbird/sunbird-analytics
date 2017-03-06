package org.ekstep.analytics.updater

import org.ekstep.analytics.framework.IJob
import optional.Application
import org.apache.spark.SparkContext
import org.ekstep.analytics.framework.JobDriver
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.JobConfig
import org.ekstep.analytics.framework.JobContext
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.util.CommonUtil._
import org.ekstep.analytics.framework.conf.AppConf
import org.ekstep.analytics.framework.Period._
import org.ekstep.analytics.framework._
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTime
import org.apache.spark.rdd.RDD
import scala.concurrent.duration._
import com.pygmalios.reactiveinflux._
import com.datastax.spark.connector._
case class CreationMerics(template_id: Option[String] = None, concept_id: Option[String] = None, asset_id: Option[String] = None, contents: Int, items: Option[Int] = None) extends Input with AlgoInput with AlgoOutput with Output
object CreationMetricsUpdater extends IBatchModelTemplate[CreationMerics, CreationMerics, CreationMerics, CreationMerics] with Serializable {
    
    val TEMPLATE = "template";
    val CONTENT = "contents";
    val ASSET = "asset"
    implicit val className = "org.ekstep.analytics.updater.CreationMetricsUpdater"
    override def name: String = "CreationMetricsUpdater"

    override def preProcess(data: RDD[CreationMerics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CreationMerics] = {
        data
    }

    override def algorithm(data: RDD[CreationMerics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CreationMerics] = {
        import com.pygmalios.reactiveinflux.spark._
        implicit val params = ReactiveInfluxDbName(AppConf.getConfig("reactiveinflux.database"))
        implicit val awaitAtMost = Integer.parseInt(AppConf.getConfig("reactiveinflux.awaitatmost")).second
        val metrics = data.map { x =>
            if (x.asset_id != None) {
                Point(time = DateTime.now().withTimeAtStartOfDay(), measurement = ASSET, tags = Map("env" -> AppConf.getConfig("application.env"),"asset_id" -> x.asset_id.get), fields = Map("contents" -> x.contents.toDouble))
            } else if (x.template_id != None) {
                Point(time = DateTime.now().withTimeAtStartOfDay(), measurement = TEMPLATE, tags = Map("env" -> AppConf.getConfig("application.env"), "template_id" -> x.template_id.get), fields = Map("contents" -> x.contents.toDouble, "items" -> x.items.get.toDouble))
            } else {
                Point(time = DateTime.now().withTimeAtStartOfDay(), measurement = CONTENT, tags = Map("env" -> AppConf.getConfig("application.env"), "template_id" -> x.concept_id.get), fields = Map("contents" -> x.contents.toDouble, "items" -> x.items.get.toDouble))
            }
        }
        metrics.saveToInflux()
        data
    }

    override def postProcess(data: RDD[CreationMerics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CreationMerics] = {
        data
    }
}