package org.ekstep.ilimi.analytics.framework.adapter

import org.ekstep.ilimi.analytics.framework.Item
import org.ekstep.ilimi.analytics.framework.ItemSet
import org.ekstep.ilimi.analytics.framework.Questionnaire
import org.ekstep.ilimi.analytics.framework.util.RestUtil
import org.ekstep.ilimi.analytics.framework.Response
import org.ekstep.ilimi.analytics.framework.util.Constants
import org.ekstep.ilimi.analytics.framework.exception.DataAdapterException
import org.ekstep.ilimi.analytics.framework.exception.DataAdapterException
import scala.collection.mutable.ListBuffer
import org.ekstep.ilimi.analytics.framework.Questionnaire
import org.ekstep.ilimi.analytics.framework.ItemSet
import org.ekstep.ilimi.analytics.framework.Item
import org.ekstep.ilimi.analytics.framework.Search
import org.ekstep.ilimi.analytics.framework.Request
import org.ekstep.ilimi.analytics.framework.SearchFilter
import org.ekstep.ilimi.analytics.framework.Metadata
import org.ekstep.ilimi.analytics.framework.util.JSONUtils

/**
 * @author Santhosh
 */
object ItemAdapter {

    val relations = Array("concepts", "questionnaires", "item_sets", "items");

    /**
     *
     */
    def getItem(itemId: String, subject: String): Item = {
        val ir = RestUtil.get[Response](Constants.getItemSetAPIUrl(itemId, subject));
        if (ir.responseCode.ne("OK")) {
            throw new DataAdapterException(ir.params.errmsg.asInstanceOf[String]);
        }
        val item = ir.result.assessment_item.get;
        getItemWrapper(item);
    }
    
    private def getItemWrapper(item: Map[String, AnyRef]) : Item = {
        Item(item.get("identifier").get.asInstanceOf[String], item.filter(p => relations.contains(p._1)), getTags(item), None);
    }

    def getItems(contentId: String): Array[Item] = {
        val cr = RestUtil.get[Response](Constants.getContentAPIUrl(contentId));
        if (cr.responseCode.ne("OK")) {
            throw new DataAdapterException(cr.params.errmsg.asInstanceOf[String]);
        }
        val content = cr.result.content.get;
        val questionnaires = content.getOrElse("questionnaires", null);
        val subject = content.getOrElse("subject", "numeracy").asInstanceOf[String];
        if (questionnaires != null) {
            var items = ListBuffer[String]();
            questionnaires.asInstanceOf[Array[Map[String, String]]].foreach(f => {
                val qr = RestUtil.get[Response](Constants.getQuestionnaireAPIUrl(f.get("identifier").get, subject));
                if (qr.responseCode.ne("OK")) {
                    throw new DataAdapterException(qr.params.errmsg.asInstanceOf[String]);
                }
                val questionnaire = qr.result.questionnaire.get;
                val itemSet = questionnaire.getOrElse("items", Map[String, AnyRef]()).asInstanceOf[Map[String, Array[String]]];
                itemSet.map(f => {
                    items ++= f._2;
                    f;
                })
            });
            items.map { x => getItem(x, subject) }.toArray;
        } else {
            null;
        }
    }

    def searchItems(itemIds: Array[String], subject: String): Array[Item] = {
        val search = Search(Request(Metadata(Array(SearchFilter("identifier", "in", Option(itemIds)))), itemIds.length));
        val sr = RestUtil.post[Response](Constants.getSearchItemAPIUrl(subject), JSONUtils.serialize(search));
        if (sr.responseCode.ne("OK")) {
            throw new DataAdapterException(sr.params.errmsg.asInstanceOf[String]);
        }
        val items = sr.result.assessment_items.getOrElse(null);
        if (null != items && items.nonEmpty) {
            items.map(f => getItemWrapper(f));
        } else {
            null;   
        }
    }

    def getItemSet(itemSetId: String, subject: String): ItemSet = {
        val isr = RestUtil.get[Response](Constants.getItemSetAPIUrl(itemSetId, subject));
        if (isr.responseCode.ne("OK")) {
            throw new DataAdapterException(isr.params.errmsg.asInstanceOf[String]);
        }
        val itemSet = isr.result.assessment_item_set.get;
        val metadata = itemSet.filter(p => relations.contains(p._1));
        val items = itemSet.getOrElse("items", Array[Map[String, AnyRef]]()).asInstanceOf[Array[Map[String, AnyRef]]].map(f => {
            getItem(f.get("id").get.asInstanceOf[String], subject);
        });
        ItemSet(itemSetId, metadata, items, getTags(itemSet), items.length);
    }

    def getItemSets(contentId: String, subject: String): Array[ItemSet] = {
        null;
    }

    @throws(classOf[DataAdapterException])
    def getQuestionnaire(questionnaireId: String, subject: String): Questionnaire = {

        val qr = RestUtil.get[Response](Constants.getQuestionnaireAPIUrl(questionnaireId, subject));
        if (qr.responseCode.ne("OK")) {
            throw new DataAdapterException(qr.params.errmsg.asInstanceOf[String]);
        }
        val questionnaire = qr.result.questionnaire.get;
        val metadata = questionnaire.filter(p => relations.contains(p._1));
        val itemSets = questionnaire.getOrElse("item_sets", Array[Map[String, AnyRef]]()).asInstanceOf[Array[Map[String, AnyRef]]].map(f => {
            getItemSet(f.get("id").get.asInstanceOf[String], subject);
        });
        var items = ListBuffer[Item]();
        itemSets.foreach { x => items ++= x.items }
        Questionnaire(questionnaireId, metadata, itemSets, items.toArray, getTags(questionnaire));
    }

    @throws(classOf[DataAdapterException])
    def getQuestionnaires(contentId: String): Array[Questionnaire] = {

        val cr = RestUtil.get[Response](Constants.getContentAPIUrl(contentId));
        if (cr.responseCode.ne("OK")) {
            throw new DataAdapterException(cr.params.errmsg.asInstanceOf[String]);
        }
        val content = cr.result.content.get;
        val questionnaires = content.getOrElse("questionnaires", null);
        val subject = content.getOrElse("subject", "numeracy").asInstanceOf[String];
        if (questionnaires != null) {
            questionnaires.asInstanceOf[Array[Map[String, String]]].map(f => {
                getQuestionnaire(f.get("identifier").get, subject);
            });
        } else {
            null;
        }
    }

    private def getTags(metadata: Map[String, AnyRef]): Option[Array[String]] = {
        Option(metadata.getOrElse("tags", Array[String]()).asInstanceOf[Array[String]]);
    }
}