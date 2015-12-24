package org.ekstep.ilimi.analytics.framework

import org.apache.spark.rdd.RDD
import org.ekstep.ilimi.analytics.framework.util.CommonUtil
import org.ekstep.ilimi.analytics.framework.exception.DataFilterException
import scala.util.control.Breaks
import org.apache.commons.beanutils.BeanUtils
import org.apache.commons.beanutils.PropertyUtils
import org.ekstep.ilimi.analytics.framework.filter.Matcher

/**
 * @author Santhosh
 */
object DataFilter {

    /**
     * Execute multiple filters
     */
    @throws(classOf[DataFilterException])
    def filterAndSort[T](events: RDD[T], filters: Option[Array[Filter]], sort: Option[Sort])(implicit mf: Manifest[T]): RDD[T] = {
        Console.println("### Running the filter process ###");
        if (filters.nonEmpty) {
            events.filter { event =>
                var valid = true;
                Breaks.breakable {
                    filters.get.foreach { filter =>
                        val value = getValue(event, filter.name);
                        valid = Matcher.compare(value, filter);
                        if (!valid) Breaks.break;
                    }
                }
                valid;
            }
        } else {
            events;
        }
    }

    /*
    @throws(classOf[DataFilterException])
    def getValue(event: Event, name: String): AnyRef = {
        name match {
            case "eventId" =>
                CommonUtil.getEventId(event);
            case "ts" =>
                CommonUtil.getEventDate(event);
            case "gameId" =>
                var gid = event.edata.eks.gid.getOrElse(null);
                if (null == gid)
                    gid = CommonUtil.getGameId(event);
                gid;
            case "gameVersion" =>
                CommonUtil.getGameVersion(event);
            case "userId" =>
                CommonUtil.getUserId(event);
            case "sessionId" =>
                event.sid.getOrElse(null);
            case "telemetryVersion" =>
                event.ver.getOrElse(null);
            case "itemId" =>
                event.edata.eks.qid.getOrElse(null);
            case _ =>
                throw new DataFilterException("Unknown filter key found");
        }
    }*/

    @throws(classOf[DataFilterException])
    def getValue(event: Any, name: String): AnyRef = {
        name match {
            case "eventId" => getBeanProperty(event, "eid");
            case "ts"      => getBeanProperty(event, "ts");
            case "gameId" =>
                var gid = getBeanProperty(event, "edata.eks.gid");
                if (null == gid)
                    gid = getBeanProperty(event, "gdata.id");
                gid;
            case "gameVersion"      => getBeanProperty(event, "gdata.ver");
            case "userId"           => getBeanProperty(event, "uid");
            case "sessionId"        => getBeanProperty(event, "sid");
            case "telemetryVersion" => getBeanProperty(event, "ver");
            case "itemId"           => getBeanProperty(event, "edata.eks.qid");
            case _                  => getBeanProperty(event, name);
        }
    }

    def getBeanProperty(event: Any, prop: String): AnyRef = {
        val obj = PropertyUtils.getProperty(event, prop);
        if (null != obj) {
            val objClass = obj.getClass.getName;
            objClass match {
                case "scala.Some" =>
                    obj.asInstanceOf[Some[AnyRef]].get;
                case "scala.None" => null;
                case _            => obj.asInstanceOf[AnyRef];
            }
        } else {
            obj;
        }
    }

}