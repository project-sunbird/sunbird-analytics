package org.ekstep.analytics.model

import com.datastax.spark.connector._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.adapter.{ContentAdapter, ContentFetcher}
import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, JobLogger}
import org.ekstep.analytics.util.Constants
import org.joda.time.DateTime

import scala.util.{Failure, Try}

case class ContentFlatList(contentId: String, metric: List[Map[String, Any]])

case class ETBCoverageOutput(contentId: String, objectType: String, totalDialcodeAttached: Int, totalDialcode: List[Map[String, Any]], totalDialcodeLinkedToContent: Int, level: Int) extends AlgoOutput

case class ContentHierarchyModel(mimeType: String, contentType: String, dialcodes: Option[List[String]], identifier: String, channel: String, status: String, name: String, content: Map[String, AnyRef], children: Option[List[ContentHierarchyModel]]) extends AlgoInput

object ETBCoverageSummaryModel extends IBatchModelTemplate[Empty, ContentHierarchyModel, ETBCoverageOutput, GraphUpdateEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.model.ETBCoverageSummaryModel"

    override def name: String = "ETBCoverageSummaryModel"

    def getPublishedTextbooks(config: Map[String, AnyRef], contentAdapter: ContentFetcher): List[String] = {
        // format date to ISO format
        val fromDate = config.getOrElse("fromDate", new DateTime().toString(CommonUtil.dateFormat)).asInstanceOf[String]
        val startDate = CommonUtil.ISTDateTimeFormatter.parseDateTime(fromDate).toDateTimeISO.toString

        val toDate = config.getOrElse("toDate", new DateTime().toString(CommonUtil.dateFormat)).asInstanceOf[String]
        val endDate = CommonUtil.ISTDateTimeFormatter.parseDateTime(toDate).plusHours(23).plusMinutes(59).plusSeconds(59).toDateTimeISO.toString

        JobLogger.log("Started executing Job ETBCoverageSummaryModel")

        // Get all last updated "Live" Textbooks within given period
        val response = contentAdapter.getTextbookContents(startDate.toString, endDate.toString)
        response.map(_.getOrElse("identifier", "").asInstanceOf[String]).toList
    }

    def getContentsFromCassandra(contentIds: List[String])(implicit sc: SparkContext): RDD[ContentHierarchyModel] = {
        val hierarchyData = sc.cassandraTable[ContentHierarchyTable](Constants.HIERARCHY_STORE_KEY_SPACE_NAME, Constants.CONTENT_HIERARCHY_TABLE).where("identifier IN ?", contentIds)
        val data = hierarchyData.map(data => data.hierarchy)

        var model: RDD[ContentHierarchyModel] = sc.emptyRDD[ContentHierarchyModel]
        Try {
            model = data.map(jsonString => JSONUtils.deserialize[ContentHierarchyModel](jsonString))
        }.recover {
            case exception => JobLogger.log("unable to parse JSON hierarchy content")
                Failure(exception)
        }

        model.filter(content => content.contentType == "TextBook") // filter only Textbooks
    }

    override def preProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[ContentHierarchyModel] = {
        val textbookIds = getPublishedTextbooks(config, ContentAdapter)
        getContentsFromCassandra(textbookIds)(sc)
    }

    def getDialcodesByLevel(content: ContentHierarchyModel): List[String] = {
        val dialcodes = content.dialcodes.getOrElse(List())
        dialcodes ::: content.children.getOrElse(List()).flatMap(x => x.dialcodes.getOrElse(List()))
    }

    def getContentCountLinkedToDialcode(content: ContentHierarchyModel): Int = {
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
                "totalDialcode" -> getDialcodesByLevel(content).groupBy(identity).map(t => Map("dialcodeId" -> t._1, "contentLinked"-> t._2.size)),
                "totalDialcodeAttached" -> getDialcodesByLevel(content).distinct.size,
                "totalDialcodeLinkedToContent" -> getContentCountLinkedToDialcode(content),
                "mimeType" -> content.mimeType,
                "level" -> level
            )
        )
    }

    def computeMetrics(data: RDD[ContentHierarchyModel]): RDD[Map[String, Any]] = {
        var mapper: List[Map[String, Any]] = List()
        val collectionMimetype = "application/vnd.ekstep.content-collection"

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
                "Content",          // collection mimeType has objectType "Content"
                metric.getOrElse("totalDialcodeAttached", 0).asInstanceOf[Int],
                metric.getOrElse("totalDialcode", List()).asInstanceOf[List[Map[String, Any]]],
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