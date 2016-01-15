package org.ekstep.analytics.framework.adapter

import org.ekstep.analytics.framework.Game
import org.ekstep.analytics.framework.Response
import org.ekstep.analytics.framework.util.RestUtil
import org.ekstep.analytics.framework.util.Constants
import org.ekstep.analytics.framework.exception.DataAdapterException
import org.ekstep.analytics.framework.Game

/**
 * @author Santhosh
 */
object ContentAdapter {

    @throws(classOf[DataAdapterException])
    def getGameList(): Array[Game] = {
        val cr = RestUtil.post[Response](Constants.getGameList, "{\"request\": {}}");
        if (!cr.responseCode.equals("OK")) {
            throw new DataAdapterException(cr.responseCode);
        }
        val games = cr.result.games.get;
        games.map(f => {
            Game(f.getOrElse("identifier", null).asInstanceOf[String], f.getOrElse("code", null).asInstanceOf[String],
                f.getOrElse("subject", null).asInstanceOf[String], f.getOrElse("objectType", null).asInstanceOf[String])
        });
    }
    
    def main(args: Array[String]): Unit = {
        val games = getGameList();
        games.foreach { x => println(x) };
    }

}