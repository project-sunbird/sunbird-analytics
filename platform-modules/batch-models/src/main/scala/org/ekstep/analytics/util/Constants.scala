package org.ekstep.analytics.util

import org.ekstep.analytics.framework.conf.AppConf
import java.net.URLEncoder

object Constants {

    val KEY_SPACE_NAME = "learner_db";
    val LEARNER_SNAPSHOT_TABLE = "learnersnapshot";
    val LEARNER_PROFICIENCY_TABLE = "learnerproficiency";
    val LEARNER_CONTENT_SUMMARY_TABLE = "learnercontentsummary";
    val LEARNER_CONCEPT_RELEVANCE_TABLE = "learnerconceptrelevance";
    val CONCEPT_SIMILARITY_TABLE = "conceptsimilaritymatrix";
    val LEARNER_PROFILE_TABLE = "learnerprofile";
    val DEVICE_KEY_SPACE_NAME = "device_db";
    val DEVICE_SPECIFICATION_TABLE = "device_specification";
    val DEVICE_USAGE_SUMMARY_TABLE = "device_usage_summary";
    val DEVICE_CONTENT_SUMMARY_FACT = "device_content_summary_fact";
    val DEVICE_RECOS = "device_recos";
    val CONTENT_KEY_SPACE_NAME = "content_db";
    val PLATFORM_KEY_SPACE_NAME = "platform_db";
    val CONTENT_STORE_KEY_SPACE_NAME = "content_store";
    val CONTENT_DATA_TABLE = "content_data";
    val CONTENT_CUMULATIVE_SUMMARY_TABLE = "content_cumulative_summary";
    val CONTENT_CUMULATIVE_METRICS_TABLE = "content_usage_metrics";
    val CONTENT_USAGE_SUMMARY_FACT = "content_usage_summary_fact";
    val CONTENT_POPULARITY_SUMMARY_FACT = "content_popularity_summary_fact";
    val GENIE_LAUNCH_SUMMARY_FACT = "genie_launch_summary_fact";
    val ITEM_USAGE_SUMMARY_FACT = "item_usage_summary_fact";
    val PUBLISH_PIPELINE_SUMMARY_FACT = "publish_pipeline_summary_fact";
    val CONTENT_SIDELOADING_SUMMARY = "content_sideloading_summary";
    val CONTENT_TO_VEC = "content_to_vector";
    val RECOMMENDATION_CONFIG = "recommendation_config";
    val JOB_REQUEST = "job_request";
    val CONTENT_RECOS = "content_recos";
    val REGISTERED_TAGS = "registered_tags";
    val JOB_CONFIG = "job_config";
    val REQUEST_RECOS = "request_recos";
    val CONTENT_SNAPSHOT_SUMMARY = "content_snapshot_summary";
    val CONCEPT_SNAPSHOT_SUMMARY = "concept_snapshot_summary";
    val ASSET_SNAPSHOT_SUMMARY = "asset_snapshot_summary";

    /* Creation tables and keyspaces */
    val CREATION_KEY_SPACE_NAME = "creation_db";
    val APP_OBJECT_CACHE_TABLE = "app_object_cache";
    val USER_PROFILE_TABLE = "user_profile";
    val CREATION_METRICS_KEY_SPACE_NAME = "creation_metrics_db"
    val CONTENT_CREATION_TABLE = "content_creation_metrics_fact";
    val CE_USAGE_SUMMARY = "ce_usage_summary_fact";
    val APP_USAGE_SUMMARY_FACT = "app_usage_summary_fact";
    val AUTHOR_USAGE_METRICS_FACT = "author_usage_summary_fact";
    val TEXTBOOK_SNAPSHOT_METRICS_TABLE = "textbook_snapshot_metrics";
    val TEXTBOOK_SESSION_METRICS_FACT = "textbook_metrics_summary_fact";
    val PLUGIN_SNAPSHOT_METRICS_TABLE = "plugin_snapshot_metrics";

    val DEFAULT_APP_ID = "EkstepPortal";

    val LP_URL = AppConf.getConfig("lp.url");
    val SEARCH_SERVICE_URL = AppConf.getConfig("service.search.url");

    def getContentList(): String = {
        s"$LP_URL/v2/analytics/content/list";
    }

    def getContent(contentId: String): String = {
        s"$LP_URL/v2/content/" + URLEncoder.encode(contentId, "UTF-8");
    }

    def getDomainMap(): String = {
        s"$LP_URL/v2/analytics/domain/map";
    }

    def getContentSearch(): String = {
        s"$SEARCH_SERVICE_URL/v2/search";
    }

    def getContentItems(apiVersion: String, contentId: String): String = {
        s"$LP_URL/$apiVersion/analytics/items/" + URLEncoder.encode(contentId, "UTF-8");
    }

    def getItemConcept(version: String, contentId: String, itemId: String): String = {
        s"$LP_URL/$version/analytics/item/$contentId/$itemId";
    }

    def getContentUpdateAPIUrl(contentId: String): String = {
        s"$LP_URL/v2/content/$contentId";
    }
}