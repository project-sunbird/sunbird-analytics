package org.ekstep.analytics.model

import org.apache.hadoop.fs.FileSystem
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Column
import org.ekstep.analytics.framework.dispatcher.AzureDispatcher
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework._


case class DruidOutput(state: Option[String], producer_id: Option[String], total_scans: Option[Integer], total_sessions: Option[Integer], context_pdata_id: Option[String], context_pdata_pid: Option[String], total_duration: Option[Double],
                       count: Option[Int], dimensions_channel: Option[String], dimensions_sid: Option[String], dimensions_pdata_id: Option[String],
                       dimensions_type: Option[String], dimensions_mode: Option[String], dimensions_did: Option[String], object_id: Option[String],
                       content_board: Option[String], total_ts: Option[Double]) extends Input with AlgoInput with AlgoOutput with Output

case class ReportConfig(id: String, queryType: String, dateRange: QueryDateRange, metrics: List[Metrics], labels: Map[String, String], output: List[OutputConfig])
case class QueryDateRange(interval: Option[QueryInterval], staticInterval: Option[String], granularity: Option[String])
case class QueryInterval(startDate: String, endDate: String)
case class Metrics(metric: String, label: String, druidQuery: DruidQueryModel)
case class OutputConfig(`type`: String, metrics: List[String], dims: List[String], filePattern: String)

object DruidQueryProcessingModel  extends IBatchModelTemplate[DruidOutput, DruidOutput, DruidOutput, DruidOutput] with Serializable {

    implicit val className = "org.ekstep.analytics.model.DruidQueryProcessingModel"
    override def name: String = "DruidQueryProcessingModel"

    override def preProcess(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DruidOutput] = {
        data
    }

    override def algorithm(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DruidOutput] = {
        val strtConfig = config.get("reportConfig").get.asInstanceOf[Map[String, AnyRef]]
        val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(strtConfig))
        val dims = reportConfig.output.map(f => f.dims).flatMap(f => f)

        val metrics = reportConfig.metrics.map{f =>
            val data = DruidDataFetcher.getDruidData(f.druidQuery)
            data.map{ x =>
                val dataMap = JSONUtils.deserialize[Map[String, AnyRef]](x)
                val key = dataMap.filter(m => dims.contains(m._1)).values.mkString(",")
                (key, dataMap)
            }
        }.flatMap(f => f)
        val finalResult = sc.parallelize(metrics).foldByKey(Map())(_ ++ _)
        finalResult.map{f => JSONUtils.deserialize[DruidOutput](JSONUtils.serialize(f._2))}
    }

    override def postProcess(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DruidOutput] = {

        val configMap = config.get("reportConfig").get.asInstanceOf[Map[String, AnyRef]]
        val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))
        val labelsLookup = reportConfig.labels
        val dimFields = reportConfig.metrics.map(m => m.druidQuery.dimensions.get.map(f => f._2)).flatMap(f => f)
        val metricFields = reportConfig.metrics.map(m => m.druidQuery.aggregations.get.map(f => f.name)).flatMap(f => f)
        implicit val sqlContext = new SQLContext(sc);
        import sqlContext.implicits._

        reportConfig.output.map{ f =>
            if("csv".equalsIgnoreCase(f.`type`)) {
                val df = data.toDF()
                val fieldsList = (dimFields ++ metricFields).distinct
                val dimsLabels = labelsLookup.filter(x => f.dims.contains(x._1)).values.toList
                val filteredDf = df.select(fieldsList.head, fieldsList.tail:_*)
                val renamedDf =  filteredDf.select(filteredDf.columns.map(c => filteredDf.col(c).as(labelsLookup.getOrElse(c, c))): _*)
                //renamedDf.show(150)
                AzureDispatcher.dispatch(config ++ Map("dims" -> dimsLabels, "report-id" -> reportConfig.id, "filePattern" -> f.filePattern), renamedDf)
            }
            else {
                val strData = data.map(f => JSONUtils.serialize(f))
                strData.foreach(f => println(f))
                AzureDispatcher.dispatch(strData.collect(), config)
            }
        }
        data
    }
}
