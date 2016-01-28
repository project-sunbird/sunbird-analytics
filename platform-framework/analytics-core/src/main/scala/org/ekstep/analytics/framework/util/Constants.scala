package org.ekstep.analytics.framework.util

import org.ekstep.analytics.framework.conf.AppConf

/**
 * @author Santhosh
 */
object Constants {

    val LP_URL = AppConf.getConfig("lp.url");

    def getContentAPIUrl(contentId: String): String = {
        s"$LP_URL/taxonomy-service/v1/content/$contentId";
    }

    def getGameList(): String = {
        s"$LP_URL/taxonomy-service/v1/game/list";
    }
    
    def getContentList(): String = {
        s"$LP_URL/taxonomy-service/v2/analytics/content/list";
    }
    
    def getDomainMap(): String = {
        s"$LP_URL/taxonomy-service/v2/analytics/domain/map";
    }
    
    def getContentItems(apiVersion: String, contentId: String): String = {
        s"$LP_URL/taxonomy-service/$apiVersion/analytics/items/$contentId";
    }

    def getItemConcept(version: String, contentId: String, itemId: String): String = {
        s"$LP_URL/taxonomy-service/$version/analytics/item/$contentId/$itemId";
    }
    
    def getQuestionnaireAPIUrl(questionnaireId: String, subject: String): String = {
        s"$LP_URL/taxonomy-service/v1/questionnaire/$questionnaireId?taxonomyId=$subject";
    }

    def getItemAPIUrl(itemId: String, subject: String): String = {
        s"$LP_URL/taxonomy-service/v1/assessmentitem/$itemId?taxonomyId=$subject";
    }

    def getSearchItemAPIUrl(subject: String): String = {
        s"$LP_URL/taxonomy-service/v1/assessmentitem/search?taxonomyId=$subject";
    }

    def getItemSetAPIUrl(itemSetId: String, subject: String): String = {
        s"$LP_URL/taxonomy-service/v1/assessmentitemset/$itemSetId?taxonomyId=$subject";
    }
}