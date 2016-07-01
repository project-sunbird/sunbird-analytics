package org.ekstep.analytics.framework.util

import scala.io.Source
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClients
import org.ekstep.analytics.framework.Response
import com.fasterxml.jackson.core.JsonParseException
import org.apache.http.client.methods.HttpPatch
import org.ekstep.analytics.framework.Level._

/**
 * @author Santhosh
 */
object RestUtil {

    val className = "org.ekstep.analytics.framework.util.RestUtil"
    
    def get[T](apiURL: String)(implicit mf: Manifest[T]) = {
        val httpClient = HttpClients.createDefault();
        val request = new HttpGet(apiURL);
        request.addHeader("user-id", "analytics");
        try {
            val httpResponse = httpClient.execute(request);
            val entity = httpResponse.getEntity()
            val inputStream = entity.getContent()
            val content = Source.fromInputStream(inputStream, "UTF-8").getLines.mkString;
            inputStream.close
            JSONUtils.deserialize[T](content);
        } catch {
            case ex: Exception =>
                JobLogger.log(ex.getMessage, className, Option(ex), Option(Map("url" -> apiURL)), Option("FAILED"), ERROR)
                null.asInstanceOf[T];
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
            val inputStream = entity.getContent()
            val content = Source.fromInputStream(inputStream, "UTF-8").getLines.mkString
            inputStream.close
            JSONUtils.deserialize[T](content);
        } catch {
            case ex: JsonParseException =>
                JobLogger.log(ex.getMessage, className, Option(ex), Option(Map("url" -> apiURL, "body" -> body)), Option("FAILED"), ERROR)
                null.asInstanceOf[T];
        } finally {
            httpClient.close()
        }
    }
    
    def patch[T](apiURL: String, body: String)(implicit mf: Manifest[T]) = {
        
        val httpClient = HttpClients.createDefault();
        val request = new HttpPatch(apiURL);
        request.addHeader("user-id", "analytics");
        request.addHeader("Content-Type", "application/json");
        request.setEntity(new StringEntity(body));
        try {
            val httpResponse = httpClient.execute(request);
            val entity = httpResponse.getEntity()
            val inputStream = entity.getContent()
            val content = Source.fromInputStream(inputStream, "UTF-8").getLines.mkString
            inputStream.close
            JSONUtils.deserialize[T](content);
        } catch {
            case ex: JsonParseException =>
                JobLogger.log(ex.getMessage, className, Option(ex), Option(Map("url" -> apiURL, "body" -> body)), Option("FAILED"), ERROR)
                null.asInstanceOf[T];
        } finally {
            httpClient.close()
        }
    }
    
}