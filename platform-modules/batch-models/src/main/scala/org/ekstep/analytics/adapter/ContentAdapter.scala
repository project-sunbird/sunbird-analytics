package org.ekstep.analytics.adapter

import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.{CommonUtil, JSONUtils, RestUtil}
import org.ekstep.analytics.util.Constants

import scala.annotation.tailrec

case class ContentModel(id: String, subject: List[String], contentType: String, languageCode: List[String], gradeList: List[String] = List[String]());
case class ContentResult(count: Int, content: Option[Array[Map[String, AnyRef]]])
case class ContentResponse(id: String, ver: String, ts: String, params: Params, responseCode: String, result: ContentResult)


trait ContentFetcher {
    def search(offset: Int, limit: Int, contents: Array[Map[String, AnyRef]], action: (Int, Int) => ContentResult): Array[Map[String, AnyRef]]
    def getPublishedContentList: ContentResult
    def getTextbookContents(lastUpdatedOnMin: String, lastUpdatedOnMax: String): Array[Map[String, AnyRef]]
}
/**
 * @author Santhosh
 */
object ContentAdapter extends BaseAdapter with ContentFetcher {

    val relations = Array("concepts", "tags");

    implicit val className = "org.ekstep.analytics.adapter.ContentAdapter"

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

}