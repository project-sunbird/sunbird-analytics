package controllers

import org.ekstep.analytics.api.service.JobAPIService
import org.ekstep.analytics.api.service.JobAPIService.DataRequest
import org.ekstep.analytics.api.service.JobAPIService.DataRequestList
import org.ekstep.analytics.api.service.JobAPIService.GetDataRequest
import org.ekstep.analytics.api.service.JobAPIService.ChannelData

import akka.actor.ActorSystem
import akka.actor.Props
import akka.pattern.ask
import akka.routing.FromConfig
import context.Context
import javax.inject.Inject
import javax.inject.Singleton
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import play.api.libs.json.Json
import play.api.mvc.Action
import akka.dispatch.Await


/**
 * @author mahesh
 */

@Singleton
class JobController @Inject() (system: ActorSystem) extends BaseController {
    implicit val className = "controllers.JobController";
    val jobAPIActor = system.actorOf(Props[JobAPIService].withRouter(FromConfig()), name = "jobApiActor");

    def dataRequest() = Action.async { implicit request =>
        val body: String = Json.stringify(request.body.asJson.get);
        val result = ask(jobAPIActor, DataRequest(body, Context.sc, config)).mapTo[String];
        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }

    def getJob(clientKey: String, requestId: String) = Action.async { implicit request =>
        val result = ask(jobAPIActor, GetDataRequest(clientKey, requestId, Context.sc, config)).mapTo[String];
        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }

    def getJobList(clientKey: String) = Action.async { implicit request =>
        val limit = Integer.parseInt(request.getQueryString("limit").getOrElse(config.getString("data_exhaust.list.limit")))
        val result = ask(jobAPIActor, DataRequestList(clientKey, limit, Context.sc, config)).mapTo[String];
        result.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }

    def getTelemetry(datasetId: String, channel: String) = Action.async { implicit request =>
        val from = request.getQueryString("from").getOrElse("")
        val to = request.getQueryString("to").getOrElse(org.ekstep.analytics.api.util.CommonUtil.getToday())
        val result = ask(jobAPIActor, ChannelData(datasetId, channel, from, to, Context.sc, config)).mapTo[String];
        val res = Await.result(result, 100 second)
        res.map { x =>
            Ok(x).withHeaders(CONTENT_TYPE -> "application/json");
        }
    }
}