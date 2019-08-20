package org.ekstep.analytics.model

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.ekstep.analytics.framework.dispatcher.AzureDispatcher
import org.ekstep.analytics.framework.fetcher.DruidDataFetcher
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.exception.DruidConfigException


case class DruidOutput(date: Option[String], state: Option[String], producer_id: Option[String], total_scans: Option[Integer] = Option(0), total_sessions: Option[Integer] = Option(0),
                       total_ts: Option[Double] = Option(0.0)) extends Input with AlgoInput with AlgoOutput with Output

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
        val strConfig = config.get("reportConfig").get.asInstanceOf[Map[String, AnyRef]]
        val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(strConfig))

        val queryDims = reportConfig.metrics.map{f =>
            f.druidQuery.dimensions.getOrElse(List()).map(f => f._2)
        }.distinct

        if(queryDims.length > 1) throw new DruidConfigException("Query dimensions are not matching")

        val interval = strConfig.get("dateRange").get.asInstanceOf[Map[String, AnyRef]]
        val granularity = interval.get("granularity")
        val queryInterval = if(interval.get("staticInterval").nonEmpty) {
            interval.get("staticInterval").get.asInstanceOf[String]
        }
        else if(interval.get("interval").nonEmpty) {
            val dateRange = interval.get("interval").get.asInstanceOf[Map[String, String]]
            dateRange.get("startDate").get + "/" + dateRange.get("endDate").get
        }
        else throw new DruidConfigException("Both staticInterval and interval cannot be missing. Either of them should be specified")

        val metrics = reportConfig.metrics.map{f =>
            val queryConfig = if(granularity.nonEmpty)
                JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(f.druidQuery)) ++ Map("intervals" -> queryInterval, "granularity" -> granularity.get)
            else
                JSONUtils.deserialize[Map[String, AnyRef]](JSONUtils.serialize(f.druidQuery)) ++ Map("intervals" -> queryInterval)

            val data = DruidDataFetcher.getDruidData(JSONUtils.deserialize[DruidQueryModel](JSONUtils.serialize(queryConfig)))
            data.map{ x =>
                val dataMap = JSONUtils.deserialize[Map[String, AnyRef]](x)
                val key = dataMap.filter(m => (queryDims.flatMap(f => f) ++ List("date")).contains(m._1)).values.map(f => f.toString).toList.sorted(Ordering.String.reverse).mkString(",")
                (key, dataMap)
            }
        }.flatMap(f => f)
        val finalResult = sc.parallelize(metrics).foldByKey(Map())(_ ++ _)
        finalResult.map{f => JSONUtils.deserialize[DruidOutput](JSONUtils.serialize(f._2))}
    }

    override def postProcess(data: RDD[DruidOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[DruidOutput] = {

        val configMap = config.get("reportConfig").get.asInstanceOf[Map[String, AnyRef]]
        val reportConfig = JSONUtils.deserialize[ReportConfig](JSONUtils.serialize(configMap))
        val labelsLookup = reportConfig.labels ++ Map("date" -> "Date")
        val dimFields = reportConfig.metrics.map(m => m.druidQuery.dimensions.get.map(f => f._2)).flatMap(f => f)
        val metricFields = reportConfig.metrics.map(m => m.druidQuery.aggregations.get.map(f => f.name)).flatMap(f => f)
        implicit val sqlContext = new SQLContext(sc);
        import sqlContext.implicits._

        reportConfig.output.map{ f =>
            if("csv".equalsIgnoreCase(f.`type`)) {
                val df = data.toDF()
                val fieldsList = (dimFields ++ metricFields ++ List("date")).distinct
                val dimsLabels = labelsLookup.filter(x => f.dims.contains(x._1)).values.toList
                val filteredDf = df.select(fieldsList.head, fieldsList.tail:_*)
                val renamedDf =  filteredDf.select(filteredDf.columns.map(c => filteredDf.col(c).as(labelsLookup.getOrElse(c, c))): _*)
                //renamedDf.show(150)
                AzureDispatcher.dispatch(config ++ Map("dims" -> dimsLabels, "reportId" -> reportConfig.id, "filePattern" -> f.filePattern), renamedDf)
            }
            else {
                val strData = data.map(f => JSONUtils.serialize(f))
                AzureDispatcher.dispatch(strData.collect(), config)
            }
        }
        data
    }
}
