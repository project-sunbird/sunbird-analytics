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