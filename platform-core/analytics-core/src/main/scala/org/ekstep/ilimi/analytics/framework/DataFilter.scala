package org.ekstep.ilimi.analytics.framework

import org.apache.spark.rdd.RDD
import org.ekstep.ilimi.analytics.framework.util.CommonUtil
import org.ekstep.ilimi.analytics.framework.exception.DataFilterException
import scala.util.control.Breaks

/**
 * @author Santhosh
 */
object DataFilter {

    /**
     * Execute multiple filters
     */
    @throws(classOf[DataFilterException])
    def filterAndSort(events: RDD[Event], filters: Option[Array[Filter]], sort: Option[Sort]): RDD[Event] = {
        Console.println("### Running the filter process ###");
        if (filters.nonEmpty) {
            events.filter { event =>
                var valid = true;
                Breaks.breakable {
                    filters.get.foreach { filter =>
                        val value = getValue(event, filter.name);
                        valid = filter.operator match {
                            case "NE" =>
                                !value.equals(filter.value.getOrElse(null));
                            case "IN" =>
                                if (filter.value.isEmpty || !(filter.value.get.isInstanceOf[List[AnyRef]])) {
                                    false;
                                } else {
                                    filter.value.get.asInstanceOf[List[AnyRef]].contains(value);
                                }
                            case "ISNULL" =>
                                null == value;
                            case "ISEMPTY" =>
                                null == value || value.toString().isEmpty();
                            case "ISNOTNULL" =>
                                null != value;
                            case "ISNOTEMPTY" =>
                                null != value && value.toString().nonEmpty;
                            case _ =>
                                value.equals(filter.value.getOrElse(null));
                        }
                        if (!valid) Breaks.break;
                    }
                }
                valid;
            }
        } else {
            events;
        }
    }

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
    }
    
}