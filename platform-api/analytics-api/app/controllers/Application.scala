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
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import scala.concurrent.duration._
import scala.concurrent.Future
import javax.inject.Singleton
import javax.inject.Inject
import akka.actor.ActorSystem
import akka.pattern._
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import akka.util.Timeout
import scala.concurrent.duration._
import org.ekstep.analytics.api.exception.ClientException
import org.ekstep.analytics.api.ResponseCode


@Singleton
class Application @Inject() (system: ActorSystem) extends Controller {
	implicit val timeout: Timeout = 20 seconds;
    val contentAPIActor = system.actorOf(ContentAPIService.props, "content-api-service-actor");
    
	implicit val config = Map(
        "content2vec.content_service_url" -> play.Play.application.configuration.getString("content2vec.content_service_url"),
        "content2vec.scripts_path" -> play.Play.application.configuration.getString("content2vec.scripts_path"),
        "content2vec.enrich_content" -> play.Play.application.configuration.getString("content2vec.enrich_content"),
        "content2vec.train_model" -> play.Play.application.configuration.getString("content2vec.train_model"),
        "content2vec.content_corpus" -> play.Play.application.configuration.getString("content2vec.content_corpus"),
        "content2vec.infer_query" -> play.Play.application.configuration.getString("content2vec.infer_query"),
        "content2vec.s3_bucket" -> play.Play.application.configuration.getString("content2vec.s3_bucket"),
        "content2vec.s3_key_prefix" -> play.Play.application.configuration.getString("content2vec.s3_key_prefix"),
        "content2vec.model_path" -> play.Play.application.configuration.getString("content2vec.model_path"),
        "content2vec.kafka_topic" -> play.Play.application.configuration.getString("content2vec.kafka_topic"),
        "content2vec.kafka_broker_list" -> play.Play.application.configuration.getString("content2vec.kafka_broker_list"),
        "content2vec.infer_all" -> play.Play.application.configuration.getString("content2vec.infer_all"),
        "content2vec.corpus_path" -> play.Play.application.configuration.getString("content2vec.corpus_path"),
        "service.search.url" -> play.Play.application.configuration.getString("service.search.url"));

    def contentUsageMetrics(contentId: String) = Action { implicit request =>

        try {
            val body: String = Json.stringify(request.body.asJson.get);
            val response = ContentAPIService.getContentUsageMetrics(contentId, body)(Context.sc);
            play.Logger.info(request + " body - " + body + "\n\t => " + response)
            Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
        } catch {
            case ex: Exception =>
                ex.printStackTrace();
                Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.contentusagesummary", ex.getMessage, ResponseCode.SERVER_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
        }

    }

    def checkAPIhealth() = Action {
        val response = HealthCheckAPIService.getHealthStatus()(Context.sc)
        Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
    }

//    def contentToVec(contentId: String) = Action {
//
//        try {
//            val response = ContentAPIService.contentToVec(contentId)(Context.sc, config);
//            Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
//
//        } catch {
//            case ex: Exception =>
//                ex.printStackTrace();
//                Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.contentToVec", ex.getMessage)).withHeaders(CONTENT_TYPE -> "application/json");
//        }
//    }
    
    
    def contentToVec(contentId: String) = Action {
    	(contentAPIActor ! ContentAPIService.ContentToVec(contentId, Context.sc, config))
    	val response = JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.content-to-vec", Map("message" -> "Job submitted for content enrichment")));
		Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
//        val futureRes = Future { ContentAPIService.contentToVec(contentId)(Context.sc, config) }
//        val timeoutFuture = play.api.libs.concurrent.Promise.timeout("Rquest Accepted... Thank you ", 30.second)
//        Future.firstCompletedOf(Seq(futureRes, timeoutFuture)).map { res => Ok(res);}
    }
    
    def trainContentToVec() = Action {
//    	(contentAPIActor ! ContentAPIService.trainContentToVec());
		val response = JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.train-content-to-vec", Map("status" -> "successful")));
		Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
    }

    def recommendations() = Action { implicit request =>
        try {
            val body: String = Json.stringify(request.body.asJson.get);
            val response = RecommendationAPIService.recommendations(body)(Context.sc, config);
            play.Logger.info(request + " body - " + body + "\n\t => " + response)
            Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
        } catch {
        	case ex: ClientException =>
        		Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.recommendations", ex.getMessage, ResponseCode.CLIENT_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
            case ex: Throwable =>
                ex.printStackTrace();
                Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.recommendations", ex.getMessage, ResponseCode.SERVER_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }
}