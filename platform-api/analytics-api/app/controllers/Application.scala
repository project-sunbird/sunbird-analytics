package controllers

import org.ekstep.analytics.api.service.ContentAPIService
import org.ekstep.analytics.api.service.HealthCheckAPIService
import org.ekstep.analytics.api.service.RecommendationAPIService
import org.ekstep.analytics.api.util._
import play.api._
import play.api.mvc._
import play.api.libs.json._
import play.api.libs.functional.syntax._
import context.Context

object Application extends Controller {

    implicit val config = Map(
            "base.url" -> play.Play.application.configuration.getString("base.url"),
            "python.scripts.loc" -> play.Play.application.configuration.getString("python.scripts.loc"),
            "enriched.json.flag" -> play.Play.application.configuration.getString("enriched.json.flag"),
            "train.model" -> play.Play.application.configuration.getString("train.model"),
            "content.to.corpus.flag" -> play.Play.application.configuration.getString("content.to.corpus.flag"),
            "infer.query.flag" -> play.Play.application.configuration.getString("infer.query.flag"),
            "s3.bucket" -> play.Play.application.configuration.getString("s3.bucket"),
            "prefix" -> play.Play.application.configuration.getString("prefix"),
            "model.file.path" -> play.Play.application.configuration.getString("model.file.path"),
            "topic" -> play.Play.application.configuration.getString("topic"),
            "broker.list" -> play.Play.application.configuration.getString("broker.list")
        );

    def contentUsageMetrics(contentId: String) = Action { implicit request =>

        try {
            val body: String = Json.stringify(request.body.asJson.get);
            val response = ContentAPIService.getContentUsageMetrics(contentId, body)(Context.sc);
            play.Logger.info(request + " body - " + body + "\n\t => " + response)
            Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
        } catch {
            case ex: Exception =>
                ex.printStackTrace();
                Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.contentusagesummary", ex.getMessage)).withHeaders(CONTENT_TYPE -> "application/json");
        }

    }

    def checkAPIhealth() = Action {
        val response = HealthCheckAPIService.getHealthStatus()(Context.sc)
        Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
    }

    def contentToVec(contentId: String) = Action {

        try {
            val response = ContentAPIService.contentToVec(contentId)(Context.sc, config);
            Ok(response).withHeaders(CONTENT_TYPE -> "application/json");

        } catch {
            case ex: Exception =>
                ex.printStackTrace();
                Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.contentToVec", ex.getMessage)).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }
    
    def recommendations() = Action { implicit request =>
      try {
        val body: String = Json.stringify(request.body.asJson.get);
        val response = RecommendationAPIService.recommendations(body)(Context.sc);
        play.Logger.info(request + " body - " + body + "\n\t => " + response)
        Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
      } catch {
        case ex: Throwable => 
          ex.printStackTrace();
          Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.recommendations", ex.getMessage)).withHeaders(CONTENT_TYPE -> "application/json");
      }
    }
}