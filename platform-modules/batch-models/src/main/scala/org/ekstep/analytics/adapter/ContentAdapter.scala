package org.ekstep.analytics.adapter

import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, RestUtil}
import org.ekstep.analytics.util.Constants

import scala.annotation.tailrec

case class ContentModel(id: String, subject: List[String], contentType: String, languageCode: List[String], gradeList: List[String] = List[String]());
case class ContentResult(count: Int, content: Option[Array[Map[String, AnyRef]]])
case class ContentResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: ContentResult)


trait ContentFetcher {
    def getAllContent: Array[Content]
    def search(offset: Int, limit: Int, contents: Array[Map[String, AnyRef]], action: (Int, Int) => ContentResult): Array[Map[String, AnyRef]]
    def getPublishedContent: Array[Map[String, AnyRef]]
    def getPublishedContentList: ContentResult
    def getTextbookContents(lastUpdatedOnMin: String, lastUpdatedOnMax: String): Array[Map[String, AnyRef]]
}
/**
 * @author Santhosh
 */
object ContentAdapter extends BaseAdapter with ContentFetcher {

    val relations = Array("concepts", "tags");

    implicit val className = "org.ekstep.analytics.adapter.ContentAdapter"

    def getAllContent(): Array[Content] = {
        val cr = RestUtil.get[Response](Constants.getContentList);
        checkResponse(cr);
        val contents = cr.result.contents.getOrElse(null);
        contents.map(f => {
            getContentWrapper(f);
        })
    }

    @tailrec
    def search(offset: Int, limit: Int, contents: Array[Map[String, AnyRef]], action: (Int, Int) => ContentResult): Array[Map[String, AnyRef]] = {
        val result = action(offset, limit)
        val c = contents ++ result.content.getOrElse(Array())
        if (result.count > (offset + limit)) {
            search((offset + limit), limit, c, action)
        } else {
            c
        }
    }


    def getPublishedContent(): Array[Map[String, AnyRef]] = {
        def _searchContent(offset: Int, limit: Int): ContentResult = {
            val searchUrl = Constants.COMPOSITE_SEARCH_URL
            val request = Map("request" -> Map("filters" -> Map("objectType" -> List("Content"), "contentType" -> List("Story", "Worksheet", "Collection", "Game"), "status" -> List("Draft", "Review", "Redraft", "Flagged", "Live", "Retired", "Mock", "Processing", "FlagDraft", "FlagReview")), "exists" -> List("lastPublishedOn", "downloadUrl"), "offset" -> offset, "limit" -> limit));
            val resp = RestUtil.post[ContentResponse](searchUrl, JSONUtils.serialize(request));
            resp.result;
        }

        search(0, 200, Array[Map[String, AnyRef]](), _searchContent);
    }

    def getPublishedContentForRE(): Array[ContentModel] = {
        val contents = getPublishedContent();
        contents.map(f => ContentModel(f.getOrElse("identifier", "").asInstanceOf[String], f.getOrElse("domain", List("literacy")).asInstanceOf[List[String]], f.getOrElse("contentType", "").asInstanceOf[String], f.getOrElse("language", List[String]()).asInstanceOf[List[String]], f.getOrElse("gradeLevel", List[String]()).asInstanceOf[List[String]]))
    }

    /**
      * Which is used to get the total published contents list
      * @return ContentResult
      */
    def getPublishedContentList(): ContentResult = {
        val request =
            s"""
               |{
               |    "request": {
               |        "filters":{
               |          "contentType": "Resource"
               |        },
               |        "fields": ["identifier", "objectType", "resourceType"]
               |    }
               |}
             """.stripMargin
        RestUtil.post[ContentResponse](Constants.COMPOSITE_SEARCH_URL, request).result
    }

    def getContentWrapper(content: Map[String, AnyRef]): Content = {
        val mc = content.getOrElse("concepts", List[String]()).asInstanceOf[List[String]].toArray;
        Content(content.get("identifier").get.asInstanceOf[String], content.filterNot(p => relations.contains(p._1)), CommonUtil.getTags(content), mc);
    }

    def getTextbookContents(lastUpdatedOnMin: String, lastUpdatedOnMax: String): Array[Map[String, AnyRef]] = {

        def _searchTextbook(offset: Int, limit: Int): ContentResult = {
            val min = s""""$lastUpdatedOnMin""""
            val max = s""""$lastUpdatedOnMax""""
            val body =
                s"""
                   |{
                   |   "request": {
                   |      "filters": {
                   |          "objectType": "Content",
                   |          "contentType": "TextBook",
                   |          "lastPublishedOn": {"min": $min , "max":  $max }
                   |       },
                   |       "fields": ["identifier"],
                   |       "limit": $limit,
                   |       "offset": $offset
                   |   }
                   |}
         """.stripMargin
            RestUtil.post[ContentResponse](Constants.COMPOSITE_SEARCH_URL, body).result
        }

        search(0, 200, Array[Map[String, AnyRef]](), _searchTextbook)
    }

    def getItemWrapper(item: Map[String, AnyRef]): Item = {
        val mc = item.getOrElse("concepts", List[String]()).asInstanceOf[List[String]].toArray;
        Item(item.get("identifier").get.asInstanceOf[String], item.filterNot(p => relations.contains(p._1)), CommonUtil.getTags(item), Option(mc), None);
    }

}