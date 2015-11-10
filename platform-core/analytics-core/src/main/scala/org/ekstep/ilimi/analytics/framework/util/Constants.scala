package org.ekstep.ilimi.analytics.framework.util

import org.ekstep.ilimi.analytics.framework.conf.AppConf

/**
 * @author Santhosh
 */
object Constants {

    val LP_URL = AppConf.getConfig("lp.url");

    def getContentAPIUrl(contentId: String): String = {
        s"$LP_URL/taxonomy-service/v1/content/$contentId";
    }

    def getQuestionnaireAPIUrl(questionnaireId: String, subject: String): String = {
        s"$LP_URL/taxonomy-service/questionnaire/$questionnaireId?taxonomyId=$subject";
    }

    def getItemAPIUrl(itemId: String, subject: String): String = {
        s"$LP_URL/taxonomy-service/assessmentitem/$itemId?taxonomyId=$subject";
    }

    def getSearchItemAPIUrl(subject: String): String = {
        s"$LP_URL/taxonomy-service/assessmentitem/search?taxonomyId=$subject";
    }

    def getItemSetAPIUrl(itemSetId: String, subject: String): String = {
        s"$LP_URL/taxonomy-service/assessmentitemset/$itemSetId?taxonomyId=$subject";
    }
}