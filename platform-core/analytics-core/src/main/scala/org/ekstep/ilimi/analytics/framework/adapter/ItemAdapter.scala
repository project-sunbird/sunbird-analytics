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
        val metadata = item.filter(p => relations.contains(p._1));
        Item(itemId, metadata, getTags(item), None);
    }

    def getItems(contentId: String): Array[Item] = {
        null;
    }

    def searchItems(itemIds: Array[String]): Array[Item] = {
        null;
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