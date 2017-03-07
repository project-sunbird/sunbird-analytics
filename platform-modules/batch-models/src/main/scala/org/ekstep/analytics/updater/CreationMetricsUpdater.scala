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
case class CreationMetrics(template_id: Option[String] = None, concept_id: Option[String] = None, asset_id: Option[String] = None, contents: Option[Int] = None, items: Option[Int] = None) extends Input with AlgoInput with AlgoOutput with Output
object CreationMetricsUpdater extends IBatchModelTemplate[CreationMetrics, CreationMetrics, CreationMetrics, CreationMetrics] with Serializable {
    
    val TEMPLATE = "template_metrics";
    val CONCEPT = "concept_metrics";
    val ASSET = "asset_metrics"
    implicit val className = "org.ekstep.analytics.updater.CreationMetricsUpdater"
    override def name: String = "CreationMetricsUpdater"

    override def preProcess(data: RDD[CreationMetrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CreationMetrics] = {
        data
    }

    override def algorithm(data: RDD[CreationMetrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CreationMetrics] = {
        import com.pygmalios.reactiveinflux.spark._
        implicit val params = ReactiveInfluxDbName(AppConf.getConfig("reactiveinflux.database"))
        implicit val awaitAtMost = Integer.parseInt(AppConf.getConfig("reactiveinflux.awaitatmost")).second

        val template = data.filter { x => !(x.template_id.isEmpty) }.groupBy { x => x.template_id.get }.mapValues { x => x.map { x => ((x.contents.getOrElse(0), x.items.getOrElse(0))) }.reduce((a, b) => (a._1 + b._1, a._2 + b._2)) }.map { case (id, (content, item)) => (id, content, item) }
        val concept = data.filter { x => !(x.concept_id.isEmpty) }.groupBy { x => x.concept_id.get }.mapValues { x => x.map { x => ((x.contents.getOrElse(0), x.items.getOrElse(0))) }.reduce((a, b) => (a._1 + b._1, a._2 + b._2)) }.map { case (id, (content, item)) => (id, content, item) }
        val asset = data.filter { x => !(x.asset_id.isEmpty) }.groupBy { x => x.asset_id.get }.mapValues { x => x.map { x => x.contents.getOrElse(0) }.reduce((a, b) => (a + b)) } 
        
        val template_metrics = template.map { x => Point(time = DateTime.now().withTimeAtStartOfDay(), measurement = TEMPLATE, tags = Map("env" -> AppConf.getConfig("application.env"), "template_id" -> x._1), fields = Map("contents" -> x._2, "items" -> x._3)) }
        val concept_metrics = concept.map { x => Point(time = DateTime.now().withTimeAtStartOfDay(), measurement = CONCEPT, tags = Map("env" -> AppConf.getConfig("application.env"), "concept_id" -> x._1), fields = Map("contents" -> x._2, "items" -> x._3)) }
        val asset_metrics = asset.map { x => Point(time = DateTime.now().withTimeAtStartOfDay(), measurement = ASSET, tags = Map("env" -> AppConf.getConfig("application.env"), "asset_id" -> x._1), fields = Map("contents" -> x._2)) }
        val metrics = template_metrics.union(concept_metrics).union(asset_metrics)
        metrics.saveToInflux()
        data
    }

    override def postProcess(data: RDD[CreationMetrics], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[CreationMetrics] = {
        data
    }
}