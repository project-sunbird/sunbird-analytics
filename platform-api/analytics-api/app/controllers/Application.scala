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
import org.ekstep.analytics.framework.util.JobLogger
import org.ekstep.analytics.framework.Level._
import com.typesafe.config.Config
import com.typesafe.config.ConfigFactory
import scala.collection.JavaConverters._


@Singleton
class Application @Inject() (system: ActorSystem) extends Controller {
	implicit val className = "controllers.Application";
	implicit val timeout: Timeout = 20 seconds;
    val contentAPIActor = system.actorOf(ContentAPIService.props, "content-api-service-actor");
    implicit val config: Config = play.Play.application.configuration.underlying()
    								.withFallback(ConfigFactory.parseMap(Map("content2vec.scripts_path" -> "",
																				"python.home" -> "",
																				"content2vec.download_path" -> "/tmp",
																				"content2vec.download_file_prefix" -> "temp_",
																				"content2vec.enrich_content" -> "true",
																				"content2vec.content_corpus" -> "true",
																				"content2vec.train_model" -> "false",
																				"content2vec.s3_bucket" -> "sandbox-data-store",
																				"content2vec.model_path" -> "model",
																				"content2vec.s3_key_prefix" -> "model",
																				"content2vec.infer_all" -> "false",
																				"content2vec.corpus_path" -> "").asJava));

    def contentUsageMetrics(contentId: String) = Action { implicit request =>

        try {
            val body: String = Json.stringify(request.body.asJson.get);
            val response = ContentAPIService.getContentUsageMetrics(contentId, body)(Context.sc);
            play.Logger.info(request + " body - " + body + "\n\t => " + response)
            Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
        } catch {
        	case ex: ClientException =>
        		Ok(CommonUtil.errorResponseSerialized("ekstep.analytics.contentusagesummary", ex.getMessage, ResponseCode.CLIENT_ERROR.toString())).withHeaders(CONTENT_TYPE -> "application/json");
        }

    }

    def checkAPIhealth() = Action {
        val response = HealthCheckAPIService.getHealthStatus()(Context.sc)
        Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
    }
    
    def contentToVec(contentId: String) = Action {
    	(contentAPIActor ! ContentAPIService.ContentToVec(contentId, Context.sc, config))
    	val response = JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.content-to-vec", Map("message" -> "Job submitted for content enrichment")));
		Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
    }
    
    def contentToVecTrainModel() = Action {
    	(contentAPIActor ! ContentAPIService.ContentToVecTrainModel(Context.sc, config))
		val response = JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.content-to-vec.train.model", Map("message" -> "successful")));
		Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
    }
    
    def recommendationsTrainModel() = Action {
    	(contentAPIActor ! ContentAPIService.RecommendationsTrainModel(Context.sc, config))
    	val response = JSONUtils.serialize(CommonUtil.OK("ekstep.analytics.recommendations.train.model", Map("message" -> "successful")));
		Ok(response).withHeaders(CONTENT_TYPE -> "application/json");
    }

    def recommendations() = Action.async { implicit request =>
        val body: String = Json.stringify(request.body.asJson.get);
        val futureRes = Future { RecommendationAPIService.recommendations(body)(Context.sc, config) };
        val timeoutFuture = play.api.libs.concurrent.Promise.timeout(CommonUtil.errorResponseSerialized("ekstep.analytics.recommendations", "request timeout", ResponseCode.REQUEST_TIMEOUT.toString()), 3.seconds);
        val firstCompleted = Future.firstCompletedOf(Seq(futureRes, timeoutFuture));
        val response: Future[String] = firstCompleted.recoverWith {
        	case ex: ClientException =>
        		Future { CommonUtil.errorResponseSerialized("ekstep.analytics.recommendations", ex.getMessage, ResponseCode.CLIENT_ERROR.toString()) };
        };
        response.map { resp => 
        	JobLogger.log("ekstep.analytics.recommendations", Option(Map("request" -> body, "response" -> resp)), INFO);
        	play.Logger.info(request + " body - " + body + "\n\t => " + resp);
        	Ok(resp).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }
}