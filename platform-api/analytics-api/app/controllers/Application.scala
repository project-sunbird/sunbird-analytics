package controllers

import org.ekstep.analytics.api.service.ContentAPIService
import play.api._
import play.api.mvc._
import play.api.libs.json._
import play.api.libs.functional.syntax._

object Application extends Controller {
    
    def index = Action {
        println("###### Index invoked ####");
        Ok("Hello World!")
    }

    def contentUsageMetrics(contentId: String) = Action { implicit request =>
        
        try {
            val body: String = if(request.body.asText.isEmpty) Json.stringify(request.body.asJson.get) else request.body.asText.get; 
            val response = ContentAPIService.getContentUsageMetrics(contentId, body);
            Ok(response).withHeaders(CONTENT_TYPE -> "application/json");    
        } catch {
            case ex: Exception =>
                ex.printStackTrace();
            Ok("Error")
        }
        
    }
}