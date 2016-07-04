package org.ekstep.analytics.adapter

import org.ekstep.analytics.framework.Item
import org.ekstep.analytics.framework.ItemSet
import org.ekstep.analytics.framework.Questionnaire
import org.ekstep.analytics.framework.util.RestUtil
import org.ekstep.analytics.framework.Response
import org.ekstep.analytics.framework.exception.DataAdapterException
import org.ekstep.analytics.framework.exception.DataAdapterException
import scala.collection.mutable.ListBuffer
import org.ekstep.analytics.framework.Questionnaire
import org.ekstep.analytics.framework.ItemSet
import org.ekstep.analytics.framework.Item
import org.ekstep.analytics.framework.Search
import org.ekstep.analytics.framework.Request
import org.ekstep.analytics.framework.SearchFilter
import org.ekstep.analytics.framework.Metadata
import org.ekstep.analytics.framework.util.JSONUtils
import scala.collection.immutable.List
import scala.collection.immutable.Map
import scala.collection.JavaConversions._
import org.ekstep.analytics.framework.util.CommonUtil
import org.ekstep.analytics.framework.ItemConcept
import org.ekstep.analytics.util.Constants

/**
 * @author Santhosh
 */
object ItemAdapter extends BaseAdapter {

    val relations = Array("concepts", "questionnaires", "item_sets", "items");

    def getItemConceptMaxScore(contentId: String, itemId: String, apiVersion: String = "v1"): ItemConcept = {

        var resMap = Map[String, AnyRef]();
        val cr = RestUtil.get[Response](Constants.getItemConcept(apiVersion, contentId, itemId));
        ItemConcept(cr.result.concepts.getOrElse(null), cr.result.maxScore.toInt);
    }

    def getItemWrapper(item: Map[String, AnyRef]): Item = {
        val mc = item.getOrElse("concepts", List[Map[String, String]]()).asInstanceOf[List[AnyRef]].map(f => f.asInstanceOf[Map[String, String]].get("identifier").get).toArray;
        Item(item.get("identifier").get.asInstanceOf[String], item.filterNot(p => relations.contains(p._1)), CommonUtil.getTags(item), Option(mc), None);
    }

}