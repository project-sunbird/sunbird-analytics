package org.ekstep.analytics.framework.adapter

import org.ekstep.analytics.framework._
import org.ekstep.analytics.framework.util.RestUtil
import org.ekstep.analytics.framework.util.Constants
import org.ekstep.analytics.framework.exception.DataAdapterException
import org.ekstep.analytics.framework.util.CommonUtil

/**
 * @author Santhosh
 */
object ContentAdapter extends BaseAdapter {
    
    val relations = Array("concepts", "tags");

    @throws(classOf[DataAdapterException])
    def getGameList(): Array[Game] = {
        val cr = RestUtil.post[Response](Constants.getGameList, "{\"request\": {}}");
        checkResponse(cr);
        val games = cr.result.games.get;
        games.map(f => {
            Game(f.getOrElse("identifier", null).asInstanceOf[String], f.getOrElse("code", null).asInstanceOf[String],
                f.getOrElse("subject", null).asInstanceOf[String], f.getOrElse("objectType", null).asInstanceOf[String])
        });
    }
    
    def getAllContent(): Array[Content] = {
        val cr = RestUtil.get[Response](Constants.getContentList);
        checkResponse(cr);
        val contents = cr.result.contents.getOrElse(null);
        if(null == contents) {
            throw new DataAdapterException("No content found");
        }
        contents.map(f => {
            getContentWrapper(f);
        })
    }
    
    private def getContentWrapper(content: Map[String, AnyRef]): Content = {
        val mc = content.getOrElse("concepts", List[String]()).asInstanceOf[List[String]].toArray;
        Content(content.get("identifier").get.asInstanceOf[String], content.filterNot(p => relations.contains(p._1)), CommonUtil.getTags(content), mc);
    }
    
    def getContentItems(contentId: String, apiVersion: String = "v1") : Array[Item] = {
        val cr = RestUtil.get[Response](Constants.getContentItems(apiVersion, contentId));
        checkResponse(cr);
        val items = cr.result.items.getOrElse(null);
        if(null == items) {
            return Array[Item]();
        }
        items.map(f => {
            getItemWrapper(f);
        })
    }
    
    private def getItemWrapper(item: Map[String, AnyRef]): Item = {
        val mc = item.getOrElse("concepts", List[String]()).asInstanceOf[List[String]].toArray;
        Item(item.get("identifier").get.asInstanceOf[String], item.filterNot(p => relations.contains(p._1)), CommonUtil.getTags(item), Option(mc), None);
    }
    
}