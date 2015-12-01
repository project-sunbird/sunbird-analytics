package org.ekstep.ilimi.analytics.framework.util

import scala.io.Source

import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.ekstep.ilimi.analytics.framework.Response

/**
 * @author Santhosh
 */
object RestUtil {

    def main(args: Array[String]): Unit = {
        val url = Constants.getContentAPIUrl("org.ekstep.story.hi.elephant");
        val response = RestUtil.get[Response](url);
        Console.println("response", response);
    }

    def get[T](apiURL: String)(implicit mf: Manifest[T]) = {
        val httpClient = HttpClients.createDefault();
        val request = new HttpGet(apiURL);
        request.addHeader("user-id", "analytics");
        try {
            val httpResponse = httpClient.execute(request);
            val entity = httpResponse.getEntity()
            val content = if (entity != null) {
                val inputStream = entity.getContent()
                val contentStr = Source.fromInputStream(inputStream, "UTF-8").getLines.mkString;
                inputStream.close
                contentStr
            } else null;
            JSONUtils.deserialize[T](content);
        } finally {
            httpClient.close()
        }
    }

    def post[T](apiURL: String, body: String)(implicit mf: Manifest[T]) = {
        
        val httpClient = HttpClients.createDefault();
        val request = new HttpPost(apiURL);
        request.addHeader("user-id", "analytics");
        request.addHeader("Content-Type", "application/json");
        request.setEntity(new StringEntity(body));
        try {
            val httpResponse = httpClient.execute(request);
            val entity = httpResponse.getEntity()
            val content = if (entity != null) {
                val inputStream = entity.getContent()
                val contentStr = Source.fromInputStream(inputStream).getLines.mkString
                inputStream.close
                contentStr;
            } else null;
            JSONUtils.deserialize[T](content);
        } finally {
            httpClient.close()
        }
    }

    private def getRestContent(url: String): String = {
        val httpClient = HttpClients.createDefault();
        val request = new HttpGet(url);
        request.addHeader("user-id", "analytics");
        try {
            val httpResponse = httpClient.execute(request);
            val entity = httpResponse.getEntity()
            if (entity != null) {
                val inputStream = entity.getContent()
                val contentStr = Source.fromInputStream(inputStream).getLines.mkString
                inputStream.close
                contentStr;
            } else null;
        } finally {
            httpClient.close()
        }
    }
}