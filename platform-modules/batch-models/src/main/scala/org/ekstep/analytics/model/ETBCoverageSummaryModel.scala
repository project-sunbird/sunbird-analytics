package org.ekstep.analytics.model

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.adapter.ContentAdapter
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.updater.GraphUpdateEvent
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTime

import scala.util.{Failure, Try}


case class ContentFlatList(contentId: String, metric: List[Map[String, Any]])

case class ETBCoverageOutput(contentId: String, objectType: String, totalDialcodeAttached: Int, totalDialcode: Map[String, Int], totalDialcodeLinkedToContent: Int, level: Int) extends AlgoOutput

case class ContentHierarchyModel(mimeType: String, contentType: String, dialcodes: Option[List[String]], identifier: String, channel: String, status: String, resourceType: String, name: String, content: Map[String, AnyRef], children: Option[List[ContentHierarchyModel]], objectType: String) extends AlgoInput

object ETBCoverageSummaryModel extends IBatchModelTemplate[Empty, ContentHierarchyModel, ETBCoverageOutput, GraphUpdateEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.ETBCoverageSummaryModel"

    override def name: String = "ETBCoverageSummaryModel"

    override def preProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentHierarchyModel] = {
        // format date to ISO format
        val fromDate = config.getOrElse("fromDate", new DateTime().toString(CommonUtil.dateFormat)).asInstanceOf[String]
        val startDate = CommonUtil.ISTDateTimeFormatter.parseDateTime(fromDate).toDateTimeISO.toString

        val toDate = config.getOrElse("toDate", new DateTime().toString(CommonUtil.dateFormat)).asInstanceOf[String]
        val endDate = CommonUtil.ISTDateTimeFormatter.parseDateTime(toDate).plusHours(23).plusMinutes(59).plusSeconds(59).toDateTimeISO.toString

        JobLogger.log("Started executing Job ETBCoverageSummaryModel")

        // Get all last updated "Live" Textbooks within given period
        val response = ContentAdapter.getTextbookContents(startDate.toString, endDate.toString)
        val contentIds = response.map(_.getOrElse("identifier", "").asInstanceOf[String])
        JobLogger.log("contents count: => " + contentIds.length)

        // Get complete Textbook Hierarchy for all the last published contents
        val hierarchyData = sc.cassandraTable[ContentHierarchyTable](Constants.HIERARCHY_STORE_KEY_SPACE_NAME, Constants.CONTENT_HIERARCHY_TABLE).where("identifier IN ?", contentIds.toList)
        val data = hierarchyData.map(data => data.hierarchy)

        var model: RDD[ContentHierarchyModel] = sc.emptyRDD[ContentHierarchyModel]
        Try {
            model = data.map(jsonString => JSONUtils.deserialize[ContentHierarchyModel](jsonString))
        }.recover {
            case exception => JobLogger.log("unable to parse JSON hierarchy content")
                Failure(exception)
        }

        model.filter(x => x.contentType == "TextBook") // filter only Textbooks
    }

    def computeMetrics(data: RDD[ContentHierarchyModel]): RDD[Map[String, Any]] = {
        var mapper: List[Map[String, Any]] = List()
        val collectionMimetype = "application/vnd.ekstep.content-collection"

        def getDialcodesByLevel(content: ContentHierarchyModel): List[String] = {
            val dialcodes = content.dialcodes.getOrElse(List())
            dialcodes ::: content.children.getOrElse(List()).flatMap(x => x.dialcodes.getOrElse(List()))
        }

        def getDialcodeLinkedToContent(content: ContentHierarchyModel): Int = {
            val count = List(content.dialcodes.getOrElse(List()).size).count(_ > 0)
            count + content.children.getOrElse(List()).map(x => x.dialcodes.getOrElse(List()).size).count(_ > 0)
        }

        def getContentMeta(content: ContentHierarchyModel, parentId: String, level: Int): List[Map[String, Any]] = {
            List(
                Map(
                    "id" -> content.identifier,
                    "parent" -> parentId,
                    "childCount" -> content.children.getOrElse(List()).length,
                    "dialcodes" -> content.dialcodes.getOrElse(List()),
                    "childrenIds" -> content.children.getOrElse(List()).map(x => x.identifier),
                    "totalDialcode" -> getDialcodesByLevel(content).groupBy(identity).map(t => (t._1, t._2.size)),
                    "totalDialcodeAttached" -> getDialcodesByLevel(content).distinct.size,
                    "totalDialcodeLinkedToContent" -> getDialcodeLinkedToContent(content),
                    "objectType" -> content.objectType,
                    "mimeType" -> content.mimeType,
                    "level" -> level
                )
            )
        }

        def flattenHierarchy(parent: ContentHierarchyModel, level: Int = 2): Unit = {
            parent.children.getOrElse(List()).foreach(content => {
                if (content.mimeType.toLowerCase.equals(collectionMimetype)) {
                    mapper :::= getContentMeta(content, parent.identifier, level)
                }
                if (content.children.getOrElse(List()).nonEmpty) flattenHierarchy(content, level + 1)
            })
        }

        data.flatMap(content => {
            mapper = List()
            flattenHierarchy(content)
            mapper ::: getContentMeta(content, content.identifier, 1)
        })
    }

    override def algorithm(input: RDD[ContentHierarchyModel], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ETBCoverageOutput] = {
        computeMetrics(input)
            .map(metric => ETBCoverageOutput(
                metric.getOrElse("id", "").asInstanceOf[String],
                metric.getOrElse("objectType", "Content").asInstanceOf[String],
                metric.getOrElse("totalDialcodeAttached", 0).asInstanceOf[Int],
                metric.getOrElse("totalDialcode", Map()).asInstanceOf[Map[String, Int]],
                metric.getOrElse("totalDialcodeLinkedToContent", 0).asInstanceOf[Int],
                metric.getOrElse("level", 0).asInstanceOf[Int]
            ))
    }

    override def postProcess(data: RDD[ETBCoverageOutput], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[GraphUpdateEvent] = {
        val output = data.map { metric =>
            val measures = Map(
                "me_totalDialcode" -> metric.totalDialcode,
                "me_totalDialcodeLinkedToContent" -> metric.totalDialcodeLinkedToContent,
                "me_totalDialcodeAttached" -> metric.totalDialcodeAttached,
                "me_hierarchyLevel" -> metric.level
            )
            val finalContentMap = measures.filter(x=> x._1.nonEmpty)
                .map{ x => x._1 -> Map("ov" -> null, "nv" -> x._2) }
            GraphUpdateEvent(DateTime.now().getMillis, metric.contentId,
                Map("properties" -> finalContentMap), metric.objectType)
        }
        output
    }
}